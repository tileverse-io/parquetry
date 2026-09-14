/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.parquetry.geotools.data;

import static io.tileverse.parquetry.internal.filter.TemporalValues.fitsUnit;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;

import org.geotools.api.filter.And;
import org.geotools.api.filter.BinaryComparisonOperator;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.FilterFactory;
import org.geotools.api.filter.Id;
import org.geotools.api.filter.MultiValuedFilter;
import org.geotools.api.filter.Not;
import org.geotools.api.filter.Or;
import org.geotools.api.filter.PropertyIsBetween;
import org.geotools.api.filter.PropertyIsEqualTo;
import org.geotools.api.filter.PropertyIsGreaterThan;
import org.geotools.api.filter.PropertyIsGreaterThanOrEqualTo;
import org.geotools.api.filter.PropertyIsLessThan;
import org.geotools.api.filter.PropertyIsLessThanOrEqualTo;
import org.geotools.api.filter.PropertyIsNotEqualTo;
import org.geotools.api.filter.PropertyIsNull;
import org.geotools.api.filter.expression.Expression;
import org.geotools.api.filter.expression.Literal;
import org.geotools.api.filter.expression.PropertyName;
import org.geotools.api.filter.identity.Identifier;
import org.geotools.api.filter.spatial.BinarySpatialOperator;
import org.geotools.api.filter.temporal.After;
import org.geotools.api.filter.temporal.AnyInteracts;
import org.geotools.api.filter.temporal.Before;
import org.geotools.api.filter.temporal.Begins;
import org.geotools.api.filter.temporal.BegunBy;
import org.geotools.api.filter.temporal.BinaryTemporalOperator;
import org.geotools.api.filter.temporal.During;
import org.geotools.api.filter.temporal.EndedBy;
import org.geotools.api.filter.temporal.Ends;
import org.geotools.api.filter.temporal.Meets;
import org.geotools.api.filter.temporal.MetBy;
import org.geotools.api.filter.temporal.OverlappedBy;
import org.geotools.api.filter.temporal.TContains;
import org.geotools.api.filter.temporal.TEquals;
import org.geotools.api.filter.temporal.TOverlaps;
import org.geotools.api.geometry.BoundingBox;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.api.temporal.Period;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.referencing.CRS;
import org.geotools.util.Converters;
import org.locationtech.jts.geom.Geometry;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.MatchAction;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.geo.JtsGeometryFilter;
import io.tileverse.parquetry.geotools.data.FeatureTypeMapper.AttributeMapping;
import io.tileverse.parquetry.geotools.data.FeatureTypeMapper.Mapping;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.ResolvedColumn;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Translates a GeoTools {@link Filter} into a parquetry {@link Predicate} plus a residual {@link Filter} for the part
 * that cannot be pushed. The predicate is a sound necessary condition of the full filter: a row the filter accepts
 * always satisfies the predicate, and the residual rejects anything the predicate lets through.
 */
final class FilterToPredicate {

    /** The pushable predicate and the residual filter GeoTools must still apply. */
    record Result(Predicate predicate, Filter residual) {}

    private enum Op {
        EQ,
        NEQ,
        LT,
        LTE,
        GT,
        GTE
    }

    private final Map<String, AttributeMapping> attributesByName;
    private final ParquetSchema schema;
    private final Optional<AttributeMapping> fidAttribute;
    private final CoordinateReferenceSystem nativeCrs;
    private final FilterFactory ff = CommonFactoryFinder.getFilterFactory();

    FilterToPredicate(Mapping mapping, CoordinateReferenceSystem nativeCrs) {
        Map<String, AttributeMapping> byName = new HashMap<>();
        for (AttributeMapping attr : mapping.attributes()) {
            byName.put(attr.name(), attr);
        }
        this.attributesByName = Map.copyOf(byName);
        this.schema = mapping.schema();
        this.fidAttribute = mapping.fidAttribute();
        this.nativeCrs = nativeCrs;
    }

    Result translate(Filter filter) {
        if (filter == null || filter == Filter.INCLUDE) {
            return new Result(Predicate.ALWAYS_TRUE, Filter.INCLUDE);
        }
        if (filter == Filter.EXCLUDE) {
            return new Result(Predicate.ALWAYS_FALSE, Filter.INCLUDE);
        }
        return switch (filter) {
            case And and -> and(and);
            case Or or -> or(or);
            case Not not -> not(not);
            case Id id -> idFilter(id);
            default ->
                leaf(filter)
                        .map(predicate -> new Result(predicate, Filter.INCLUDE))
                        .orElseGet(() -> new Result(Predicate.ALWAYS_TRUE, filter));
        };
    }

    /**
     * Pushes an {@link Id} filter as {@code featureIdColumn IN (values)} and keeps the original filter as the residual.
     * The predicate is only a necessary condition: the read still honors the {@code Id} exactly through the residual,
     * but the IN lets the STATS, dictionary, and bloom tiers prune row groups and pages first.
     *
     * <p>A feature id is the bare column value ({@link DatasetFeatureReader} stringifies it); the WFS
     * {@code typeName.<id>} convention is a protocol concern handled above the data access layer, not unwound here.
     * Each requested id is converted to the feature id column's type; ids that do not convert match no row and are
     * dropped, and an Id whose ids never convert is {@code ALWAYS_FALSE}. With no feature id column the filter cannot
     * be pushed and rides the residual unchanged.
     */
    private Result idFilter(Id id) {
        if (fidAttribute.isEmpty()) {
            return new Result(Predicate.ALWAYS_TRUE, id);
        }
        AttributeMapping fid = fidAttribute.get();
        Set<Value> values = new LinkedHashSet<>();
        for (Identifier identifier : id.getIdentifiers()) {
            Value value = toFidValue(fid.binding(), String.valueOf(identifier.getID()));
            if (value != null) {
                values.add(value);
            }
        }
        if (values.isEmpty()) {
            return new Result(Predicate.ALWAYS_FALSE, Filter.INCLUDE);
        }
        return new Result(new Predicate.In(fid.path(), List.copyOf(values)), id);
    }

    /** Converts a feature id string to the feature id column's value type, or {@code null} when it does not fit. */
    private static Value toFidValue(Class<?> binding, String text) {
        if (binding == String.class) {
            return new Value.StringVal(text);
        }
        if (binding == Long.class) {
            Long v = Converters.convert(text, Long.class);
            return v == null ? null : new Value.LongVal(v);
        }
        if (binding == Integer.class) {
            Integer v = Converters.convert(text, Integer.class);
            return v == null ? null : new Value.IntVal(v);
        }
        if (binding == Double.class) {
            Double v = Converters.convert(text, Double.class);
            return v == null ? null : new Value.DoubleVal(v);
        }
        if (binding == Float.class) {
            Float v = Converters.convert(text, Float.class);
            return v == null ? null : new Value.FloatVal(v);
        }
        if (binding == UUID.class) {
            return parseUuid(text).map(Value.UuidVal::new).orElse(null);
        }
        return null;
    }

    /** Parses a UUID from its canonical string form, or empty when the text is not a valid UUID. */
    private static Optional<UUID> parseUuid(String text) {
        try {
            return Optional.of(UUID.fromString(text));
        } catch (IllegalArgumentException _) {
            return Optional.empty();
        }
    }

    private Result and(And and) {
        List<Predicate> pushed = new ArrayList<>();
        List<Filter> residuals = new ArrayList<>();
        for (Filter child : and.getChildren()) {
            Result childResult = translate(child);
            if (childResult.predicate() != Predicate.ALWAYS_TRUE) {
                pushed.add(childResult.predicate());
            }
            if (childResult.residual() != Filter.INCLUDE) {
                residuals.add(childResult.residual());
            }
        }
        Predicate predicate = conjunction(pushed);
        Filter residual = residualConjunction(residuals);
        return new Result(predicate, residual);
    }

    // ff.and / Pred.and wrap even a single element in a composite node; unwrap a lone element here to avoid an And of
    // one child, keeping the result equal to the bare predicate or filter.
    private Filter residualConjunction(List<Filter> residuals) {
        if (residuals.isEmpty()) {
            return Filter.INCLUDE;
        }
        if (residuals.size() == 1) {
            return residuals.get(0);
        }
        return ff.and(residuals);
    }

    private static Predicate conjunction(List<Predicate> pushed) {
        if (pushed.isEmpty()) {
            return Predicate.ALWAYS_TRUE;
        }
        if (pushed.size() == 1) {
            return pushed.get(0);
        }
        return Pred.and(pushed.toArray(Predicate[]::new));
    }

    private Result or(Or or) {
        List<Predicate> pushed = new ArrayList<>();
        for (Filter child : or.getChildren()) {
            Result childResult = translate(child);
            if (childResult.residual() != Filter.INCLUDE) {
                // A disjunction cannot be split into a necessary condition; keep the whole Or as the residual.
                return new Result(Predicate.ALWAYS_TRUE, or);
            }
            pushed.add(childResult.predicate());
        }
        return new Result(Pred.or(pushed.toArray(Predicate[]::new)), Filter.INCLUDE);
    }

    private Result not(Not not) {
        Result inner = translate(not.getFilter());
        if (inner.residual() != Filter.INCLUDE) {
            return new Result(Predicate.ALWAYS_TRUE, not);
        }
        return new Result(Pred.not(inner.predicate()), Filter.INCLUDE);
    }

    private Optional<Predicate> leaf(Filter filter) {
        return switch (filter) {
            case PropertyIsEqualTo f -> comparison(f, Op.EQ);
            case PropertyIsNotEqualTo f -> comparison(f, Op.NEQ);
            case PropertyIsLessThan f -> comparison(f, Op.LT);
            case PropertyIsLessThanOrEqualTo f -> comparison(f, Op.LTE);
            case PropertyIsGreaterThan f -> comparison(f, Op.GT);
            case PropertyIsGreaterThanOrEqualTo f -> comparison(f, Op.GTE);
            case PropertyIsBetween f -> between(f);
            case PropertyIsNull f -> isNull(f);
            case BinaryTemporalOperator f -> temporal(f);
            case org.geotools.api.filter.spatial.BBOX f -> bbox(f);
            case org.geotools.api.filter.spatial.Intersects f -> spatial(f, JtsGeometryFilter::intersects);
            case org.geotools.api.filter.spatial.Contains f -> spatial(f, JtsGeometryFilter::contains);
            case org.geotools.api.filter.spatial.Within f -> spatial(f, JtsGeometryFilter::within);
            case org.geotools.api.filter.spatial.Crosses f -> spatial(f, JtsGeometryFilter::crosses);
            case org.geotools.api.filter.spatial.Overlaps f -> spatial(f, JtsGeometryFilter::overlaps);
            case org.geotools.api.filter.spatial.Touches f -> spatial(f, JtsGeometryFilter::touches);
            case org.geotools.api.filter.spatial.Disjoint f -> spatial(f, JtsGeometryFilter::disjoint);
            case org.geotools.api.filter.spatial.Equals f -> spatial(f, JtsGeometryFilter::equalsExact);
            case org.geotools.api.filter.spatial.DWithin f -> dwithin(f);
            default -> Optional.empty();
        };
    }

    private Optional<Predicate> comparison(BinaryComparisonOperator f, Op op) {
        Optional<Leaf> leaf = leafColumn(f.getExpression1());
        Optional<Object> value = literal(f.getExpression2());
        if (leaf.isEmpty() || value.isEmpty()) {
            return Optional.empty();
        }
        Leaf column = leaf.get();
        return build(column, op, value.get()).map(predicate -> column.quantify(predicate, f));
    }

    private Optional<Predicate> between(PropertyIsBetween f) {
        Optional<Leaf> leaf = leafColumn(f.getExpression());
        Optional<Object> lo = literal(f.getLowerBoundary());
        Optional<Object> hi = literal(f.getUpperBoundary());
        if (leaf.isEmpty() || lo.isEmpty() || hi.isEmpty()) {
            return Optional.empty();
        }
        Leaf column = leaf.get();
        Optional<Predicate> low = build(column, Op.GTE, lo.get());
        Optional<Predicate> high = build(column, Op.LTE, hi.get());
        if (low.isEmpty() || high.isEmpty()) {
            return Optional.empty();
        }
        // A repeated leaf must have ONE element satisfy both bounds; quantify the whole conjunction, not each bound.
        Predicate bounded = Pred.and(low.get(), high.get());
        return Optional.of(column.quantify(bounded, f));
    }

    private Optional<Predicate> isNull(PropertyIsNull f) {
        return leafColumn(f.getExpression())
                .map(column -> column.quantify(Pred.col(column.path()).isNull(), f));
    }

    /**
     * Reduces a temporal operator on a timestamp attribute to comparisons on that attribute, following the position
     * rules by which GeoTools evaluates the operator: an attribute value is an instant, and against an instant literal
     * or the bounds of a period literal each operator either compares the attribute with one bound or can never hold.
     * The attribute may sit on either side of the operator.
     *
     * <p>Three cases are left to the residual filter. A date or a time attribute, because GeoTools evaluates these
     * operators on instants while such an attribute converts to a date or a time. A repeated (list or map element)
     * leaf, because GeoTools evaluates a temporal operator on a list-valued attribute as false: its evaluation ignores
     * the match action, and a list converts to no temporal primitive. Pushing a per-element reading would answer a
     * question that the caller did not ask. An operator outside the fourteen types declared by
     * {@link PushdownFilterCapabilities}, whose position rules are unknown here.
     */
    private Optional<Predicate> temporal(BinaryTemporalOperator f) {
        Optional<Leaf> leftLeaf = leafColumn(f.getExpression1());
        boolean attributeFirst = leftLeaf.isPresent();
        Optional<Leaf> leaf = attributeFirst ? leftLeaf : leafColumn(f.getExpression2());
        Optional<Object> value = literal(attributeFirst ? f.getExpression2() : f.getExpression1());
        if (leaf.isEmpty() || value.isEmpty() || !isSingleValuedTimestamp(leaf.get())) {
            return Optional.empty();
        }
        Leaf column = leaf.get();
        Object rawValue = value.get();
        if (rawValue instanceof Period period) {
            Optional<PeriodRelation> relation = periodRelation(f, attributeFirst);
            if (relation.isEmpty()) {
                return Optional.empty();
            }
            return relation.get().toPredicate(column, period);
        }
        Optional<LocalDateTime> at = timestampLiteral(column, rawValue);
        if (at.isEmpty()) {
            return Optional.empty();
        }
        return instantRelation(f, attributeFirst, column, at.get());
    }

    /**
     * True when the column holds one timestamp per row, which is the only attribute shape on which these operators
     * reduce.
     */
    private static boolean isSingleValuedTimestamp(Leaf column) {
        Class<?> binding = column.binding();
        boolean timestamp = binding == Instant.class || binding == LocalDateTime.class;
        return timestamp && !column.repeated();
    }

    /**
     * The comparison for {@code f} against an instant literal, matching no row in a position never taken by an instant,
     * or empty for an operator outside the fourteen temporal types.
     */
    private static Optional<Predicate> instantRelation(
            BinaryTemporalOperator f, boolean attributeFirst, Leaf column, LocalDateTime at) {
        ColumnPath path = column.path();
        Value.TimestampVal value = new Value.TimestampVal(at, column.binding() == Instant.class);
        return switch (f) {
            case After _ -> Optional.of(attributeFirst ? new Predicate.Gt(path, value) : new Predicate.Lt(path, value));
            case Before _ ->
                Optional.of(attributeFirst ? new Predicate.Lt(path, value) : new Predicate.Gt(path, value));
            case TEquals _, AnyInteracts _ -> Optional.of(new Predicate.Eq(path, value));
            case Begins _,
                    BegunBy _,
                    During _,
                    EndedBy _,
                    Ends _,
                    Meets _,
                    MetBy _,
                    OverlappedBy _,
                    TContains _,
                    TOverlaps _ -> Optional.of(Predicate.ALWAYS_FALSE);
            default -> Optional.empty();
        };
    }

    /**
     * How an instant attribute must relate to the bounds of a period literal for {@code f} to hold, or empty for an
     * operator outside the fourteen temporal types.
     */
    private static Optional<PeriodRelation> periodRelation(BinaryTemporalOperator f, boolean attributeFirst) {
        if (attributeFirst) {
            return attributeFirstRelation(f);
        }
        return literalFirstRelation(f);
    }

    private static Optional<PeriodRelation> attributeFirstRelation(BinaryTemporalOperator f) {
        return switch (f) {
            case After _ -> Optional.of(PeriodRelation.AFTER_END);
            case Before _ -> Optional.of(PeriodRelation.BEFORE_BEGIN);
            case During _ -> Optional.of(PeriodRelation.STRICTLY_INSIDE);
            case Begins _ -> Optional.of(PeriodRelation.AT_BEGIN);
            case Ends _ -> Optional.of(PeriodRelation.AT_END);
            case AnyInteracts _ -> Optional.of(PeriodRelation.INSIDE_OR_AT_BOUNDS);
            case BegunBy _, EndedBy _, Meets _, MetBy _, OverlappedBy _, TContains _, TEquals _, TOverlaps _ ->
                Optional.of(PeriodRelation.NEVER);
            default -> Optional.empty();
        };
    }

    private static Optional<PeriodRelation> literalFirstRelation(BinaryTemporalOperator f) {
        return switch (f) {
            case After _ -> Optional.of(PeriodRelation.BEFORE_BEGIN);
            case Before _ -> Optional.of(PeriodRelation.AFTER_END);
            case TContains _ -> Optional.of(PeriodRelation.STRICTLY_INSIDE);
            case BegunBy _ -> Optional.of(PeriodRelation.AT_BEGIN);
            case EndedBy _ -> Optional.of(PeriodRelation.AT_END);
            case AnyInteracts _ -> Optional.of(PeriodRelation.INSIDE_OR_AT_BOUNDS);
            case Begins _, During _, Ends _, Meets _, MetBy _, OverlappedBy _, TEquals _, TOverlaps _ ->
                Optional.of(PeriodRelation.NEVER);
            default -> Optional.empty();
        };
    }

    /** The relation of an instant attribute to a period literal, as comparisons against the period's bounds. */
    private enum PeriodRelation {
        AFTER_END,
        BEFORE_BEGIN,
        STRICTLY_INSIDE,
        AT_BEGIN,
        AT_END,
        INSIDE_OR_AT_BOUNDS,
        NEVER;

        /** The comparisons on {@code column} for which this relation holds, or empty when a bound does not convert. */
        Optional<Predicate> toPredicate(Leaf column, Period period) {
            if (this == NEVER) {
                return Optional.of(Predicate.ALWAYS_FALSE);
            }
            Optional<LocalDateTime> beginAt = boundLiteral(column, period.getBeginning());
            Optional<LocalDateTime> endAt = boundLiteral(column, period.getEnding());
            if (beginAt.isEmpty() || endAt.isEmpty()) {
                return Optional.empty();
            }
            boolean adjustedToUtc = column.binding() == Instant.class;
            Value.TimestampVal begin = new Value.TimestampVal(beginAt.get(), adjustedToUtc);
            Value.TimestampVal end = new Value.TimestampVal(endAt.get(), adjustedToUtc);
            ColumnPath path = column.path();
            return Optional.of(
                    switch (this) {
                        case AFTER_END -> new Predicate.Gt(path, end);
                        case BEFORE_BEGIN -> new Predicate.Lt(path, begin);
                        case STRICTLY_INSIDE -> Pred.and(new Predicate.Gt(path, begin), new Predicate.Lt(path, end));
                        case AT_BEGIN -> new Predicate.Eq(path, begin);
                        case AT_END -> new Predicate.Eq(path, end);
                        case INSIDE_OR_AT_BOUNDS ->
                            Pred.and(new Predicate.GtEq(path, begin), new Predicate.LtEq(path, end));
                        case NEVER -> Predicate.ALWAYS_FALSE;
                    });
        }

        /**
         * A period bound as the UTC wall-clock value stored by {@code column}, or empty when the bound is absent or
         * holds a position with no date reading. A period literal of any shape leaves the translator without throwing.
         */
        private static Optional<LocalDateTime> boundLiteral(Leaf column, org.geotools.api.temporal.Instant bound) {
            if (bound == null || bound.getPosition() == null) {
                return Optional.empty();
            }
            Date date = bound.getPosition().getDate();
            return timestampLiteral(column, date);
        }
    }

    private Optional<Predicate> bbox(org.geotools.api.filter.spatial.BBOX f) {
        Optional<ColumnPath> geom = geometryColumn(f.getExpression1());
        if (geom.isEmpty()) {
            return Optional.empty();
        }
        BoundingBox bounds = f.getBounds();
        requireNativeCrs(bounds.getCoordinateReferenceSystem());
        Bbox box = Bbox.of2d(bounds.getMinX(), bounds.getMinY(), bounds.getMaxX(), bounds.getMaxY());
        return Optional.of(new Predicate.Spatial.BboxIntersects(geom.get(), box));
    }

    private Optional<Predicate> spatial(
            BinarySpatialOperator f, BiFunction<ColumnPath, Geometry, JtsGeometryFilter> factory) {
        Optional<ColumnPath> geom = geometryColumn(f.getExpression1());
        Optional<Geometry> query = queryGeometry(f.getExpression2());
        if (geom.isEmpty() || query.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(Predicate.geometryFilter(factory.apply(geom.get(), query.get())));
    }

    private Optional<Predicate> dwithin(org.geotools.api.filter.spatial.DWithin f) {
        if (f.getDistanceUnits() != null && !f.getDistanceUnits().isBlank()) {
            // Distance units are not interpreted; only the dataset's native CRS units are honored.
            return Optional.empty();
        }
        Optional<ColumnPath> geom = geometryColumn(f.getExpression1());
        Optional<Geometry> query = queryGeometry(f.getExpression2());
        if (geom.isEmpty() || query.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(
                Predicate.geometryFilter(JtsGeometryFilter.dwithin(geom.get(), query.get(), f.getDistance())));
    }

    /** The column path when {@code e} names the default geometry attribute, else empty. */
    private Optional<ColumnPath> geometryColumn(Expression e) {
        return property(e).filter(AttributeMapping::geometry).map(AttributeMapping::path);
    }

    private Optional<Geometry> queryGeometry(Expression e) {
        Optional<Object> value = literal(e);
        if (value.isEmpty() || !(value.get() instanceof Geometry g)) {
            return Optional.empty();
        }
        // A geometry literal with no attached CRS is intentionally treated as native, matching the BBOX
        // null-CRS behavior; this is a deliberate assumption, not a skipped guard.
        if (g.getUserData() instanceof CoordinateReferenceSystem literalCrs) {
            requireNativeCrs(literalCrs);
        }
        return Optional.of(g);
    }

    private void requireNativeCrs(CoordinateReferenceSystem queryCrs) {
        if (queryCrs != null && nativeCrs != null && !CRS.equalsIgnoreMetadata(queryCrs, nativeCrs)) {
            throw new IllegalArgumentException(
                    "spatial filter literal CRS does not match the dataset native CRS; reproject the query to native");
        }
    }

    private Optional<Predicate> build(Leaf column, Op op, Object rawValue) {
        ColumnPath p = column.path();
        Class<?> binding = column.binding();
        if (binding == Integer.class) {
            return buildInteger(p, op, rawValue);
        }
        if (binding == Long.class) {
            return buildLong(p, op, rawValue);
        }
        if (binding == Double.class) {
            return buildDouble(p, op, rawValue);
        }
        if (binding == Float.class) {
            return buildFloat(p, op, rawValue);
        }
        if (binding == String.class) {
            return buildString(p, op, rawValue);
        }
        if (binding == Boolean.class) {
            return buildBoolean(p, op, rawValue);
        }
        if (binding == UUID.class) {
            return buildUuid(p, op, rawValue);
        }
        if (binding == BigDecimal.class) {
            return buildDecimal(p, op, rawValue);
        }
        if (binding == LocalDate.class) {
            return buildDate(p, op, rawValue);
        }
        if (binding == Instant.class || binding == LocalDateTime.class) {
            return buildTimestamp(column, op, rawValue);
        }
        if (binding == LocalTime.class) {
            return buildTime(column, op, rawValue);
        }
        if (binding == byte[].class) {
            return buildBinary(p, op, rawValue);
        }
        return Optional.empty();
    }

    private Optional<Predicate> buildInteger(ColumnPath p, Op op, Object rawValue) {
        Integer v = Converters.convert(rawValue, Integer.class);
        if (v == null || !isLossless(rawValue, v.doubleValue())) {
            return Optional.empty();
        }
        return ordered(p, op, v.intValue());
    }

    private Optional<Predicate> buildLong(ColumnPath p, Op op, Object rawValue) {
        Long v = Converters.convert(rawValue, Long.class);
        if (v == null || !isLossless(rawValue, v.doubleValue())) {
            return Optional.empty();
        }
        return ordered(p, op, v.longValue());
    }

    private Optional<Predicate> buildDouble(ColumnPath p, Op op, Object rawValue) {
        Double v = Converters.convert(rawValue, Double.class);
        return v == null ? Optional.empty() : ordered(p, op, v.doubleValue());
    }

    private Optional<Predicate> buildFloat(ColumnPath p, Op op, Object rawValue) {
        Float v = Converters.convert(rawValue, Float.class);
        if (v == null || op != Op.EQ) {
            return Optional.empty();
        }
        return Optional.of(Pred.col(p).eq(v.floatValue()));
    }

    private Optional<Predicate> buildString(ColumnPath p, Op op, Object rawValue) {
        String v = Converters.convert(rawValue, String.class);
        if (v == null) {
            return Optional.empty();
        }
        return switch (op) {
            case EQ -> Optional.of(Pred.col(p).eq(v));
            case NEQ -> Optional.of(Pred.col(p).notEq(v));
            default -> Optional.empty();
        };
    }

    private Optional<Predicate> buildBoolean(ColumnPath p, Op op, Object rawValue) {
        Boolean v = Converters.convert(rawValue, Boolean.class);
        if (v == null || op != Op.EQ) {
            return Optional.empty();
        }
        return Optional.of(Pred.col(p).eq(v.booleanValue()));
    }

    // Only equality pushes: the physical column is unsigned FIXED_LEN_BYTE_ARRAY, and core has no notEq(UUID) nor an
    // ordered UUID comparison whose unsigned byte order would match a signed range push. Range/inequality stays
    // residual.
    private Optional<Predicate> buildUuid(ColumnPath p, Op op, Object rawValue) {
        if (op != Op.EQ) {
            return Optional.empty();
        }
        if (rawValue instanceof UUID u) {
            return Optional.of(Pred.col(p).eq(u));
        }
        return parseUuid(rawValue.toString()).map(parsed -> Pred.col(p).eq(parsed));
    }

    private static Optional<Predicate> buildDecimal(ColumnPath p, Op op, Object rawValue) {
        BigDecimal v = Converters.convert(rawValue, BigDecimal.class);
        if (v == null) {
            return Optional.empty();
        }
        return ordered(p, op, new Value.DecimalVal(v));
    }

    private static Optional<Predicate> buildDate(ColumnPath p, Op op, Object rawValue) {
        LocalDate v = Converters.convert(rawValue, LocalDate.class);
        if (v == null) {
            return Optional.empty();
        }
        return ordered(p, op, new Value.DateVal(v));
    }

    private static Optional<Predicate> buildTimestamp(Leaf column, Op op, Object rawValue) {
        boolean adjustedToUtc = column.binding() == Instant.class;
        return timestampLiteral(column, rawValue)
                .flatMap(v -> ordered(column.path(), op, new Value.TimestampVal(v, adjustedToUtc)));
    }

    private static Optional<Predicate> buildTime(Leaf column, Op op, Object rawValue) {
        return timeLiteral(column, rawValue).flatMap(v -> ordered(column.path(), op, new Value.TimeVal(v)));
    }

    // Only equality and inequality push: a byte-order comparison has no defined meaning for a GeoTools byte[]
    // attribute, and a text literal names no particular byte encoding. Both stay residual.
    private static Optional<Predicate> buildBinary(ColumnPath p, Op op, Object rawValue) {
        if (!(rawValue instanceof byte[] bytes) || (op != Op.EQ && op != Op.NEQ)) {
            return Optional.empty();
        }
        Value.BinaryVal v =
                new Value.BinaryVal(MemorySegment.ofArray(bytes.clone()).asReadOnly());
        return ordered(p, op, v);
    }

    /**
     * The timestamp literal as the UTC wall-clock value stored by the column, or empty when the literal does not
     * convert or is finer than the column unit. A value finer than the unit is rejected by the engine rather than
     * truncated, hence such a comparison stays residual. A TIMESTAMP column outside INT64 is excluded too: a timestamp
     * attribute binds on an INT64 column alone.
     */
    private static Optional<LocalDateTime> timestampLiteral(Leaf column, Object rawValue) {
        LocalDateTime v = Converters.convert(rawValue, LocalDateTime.class);
        if (v == null || column.kind() != PrimitiveKind.INT64) {
            return Optional.empty();
        }
        Optional<LogicalType.TimeUnit> unit = timestampUnit(column);
        if (unit.isEmpty() || !fitsUnit(v.getNano(), unit.get())) {
            return Optional.empty();
        }
        return Optional.of(v);
    }

    /**
     * The time literal, or empty when it does not convert or is finer than the column unit. A TIME column outside INT64
     * is excluded too: the engine decodes a time cell from an INT64 count since midnight alone.
     */
    private static Optional<LocalTime> timeLiteral(Leaf column, Object rawValue) {
        LocalTime v = Converters.convert(rawValue, LocalTime.class);
        if (v == null || column.kind() != PrimitiveKind.INT64) {
            return Optional.empty();
        }
        Optional<LogicalType.TimeUnit> unit = timeUnit(column);
        if (unit.isEmpty() || !fitsUnit(v.toNanoOfDay(), unit.get())) {
            return Optional.empty();
        }
        return Optional.of(v);
    }

    /** The unit of a TIMESTAMP column, or empty when the column has no timestamp annotation. */
    private static Optional<LogicalType.TimeUnit> timestampUnit(Leaf column) {
        LogicalType logicalType = column.logicalType().orElse(null);
        if (logicalType instanceof LogicalType.Timestamp(boolean _, LogicalType.TimeUnit unit)) {
            return Optional.of(unit);
        }
        return Optional.empty();
    }

    /** The unit of a TIME column, or empty when the column has no time annotation. */
    private static Optional<LogicalType.TimeUnit> timeUnit(Leaf column) {
        LogicalType logicalType = column.logicalType().orElse(null);
        if (logicalType instanceof LogicalType.Time(boolean _, LogicalType.TimeUnit unit)) {
            return Optional.of(unit);
        }
        return Optional.empty();
    }

    /** True when {@code rawValue} converts to {@code converted} without loss (no fractional part dropped). */
    private static boolean isLossless(Object rawValue, double converted) {
        Double asDouble = Converters.convert(rawValue, Double.class);
        return asDouble != null && asDouble.doubleValue() == converted;
    }

    // Separate per-primitive overloads, not accidental duplication: Pred.col(p) exposes primitive-typed
    // builders (eq(int)/eq(long)/eq(double)...); keeping each overload monomorphic avoids autoboxing.
    private static Optional<Predicate> ordered(ColumnPath p, Op op, int v) {
        return Optional.of(
                switch (op) {
                    case EQ -> Pred.col(p).eq(v);
                    case NEQ -> Pred.col(p).notEq(v);
                    case LT -> Pred.col(p).lt(v);
                    case LTE -> Pred.col(p).ltEq(v);
                    case GT -> Pred.col(p).gt(v);
                    case GTE -> Pred.col(p).gtEq(v);
                });
    }

    private static Optional<Predicate> ordered(ColumnPath p, Op op, long v) {
        return Optional.of(
                switch (op) {
                    case EQ -> Pred.col(p).eq(v);
                    case NEQ -> Pred.col(p).notEq(v);
                    case LT -> Pred.col(p).lt(v);
                    case LTE -> Pred.col(p).ltEq(v);
                    case GT -> Pred.col(p).gt(v);
                    case GTE -> Pred.col(p).gtEq(v);
                });
    }

    private static Optional<Predicate> ordered(ColumnPath p, Op op, double v) {
        return Optional.of(
                switch (op) {
                    case EQ -> Pred.col(p).eq(v);
                    case NEQ -> Pred.col(p).notEq(v);
                    case LT -> Pred.col(p).lt(v);
                    case LTE -> Pred.col(p).ltEq(v);
                    case GT -> Pred.col(p).gt(v);
                    case GTE -> Pred.col(p).gtEq(v);
                });
    }

    /** The comparison predicate over an already-typed value, for the bindings with no primitive builder. */
    private static Optional<Predicate> ordered(ColumnPath p, Op op, Value v) {
        return Optional.of(
                switch (op) {
                    case EQ -> new Predicate.Eq(p, v);
                    case NEQ -> new Predicate.NotEq(p, v);
                    case LT -> new Predicate.Lt(p, v);
                    case LTE -> new Predicate.LtEq(p, v);
                    case GT -> new Predicate.Gt(p, v);
                    case GTE -> new Predicate.GtEq(p, v);
                });
    }

    /**
     * A column against which a comparison builds: its physical path, its Java binding for literal coercion, whether it
     * is a repeated (list/map element) leaf that an existential quantifier must aggregate over, and the physical kind
     * plus logical type annotation that decide how a decimal or temporal literal encodes.
     */
    private record Leaf(
            ColumnPath path,
            Class<?> binding,
            boolean repeated,
            PrimitiveKind kind,
            Optional<LogicalType> logicalType) {

        /** Wraps {@code leaf} in the filter's match-action quantifier when this column is repeated; else returns it. */
        Predicate quantify(Predicate leaf, Filter filter) {
            if (repeated) {
                return new Predicate.Quantified(matchAction(filter), leaf);
            }
            return leaf;
        }
    }

    /**
     * Resolves the comparison operand to a buildable {@link Leaf}. A plain top-level scalar attribute uses the existing
     * attribute mapping; any other property (a slash- or dot-separated nested path) is resolved against the schema as a
     * nested leaf. Empty when the operand is not a property, names a geometry attribute, or does not resolve.
     */
    private Optional<Leaf> leafColumn(Expression e) {
        if (!(e instanceof PropertyName name)) {
            return Optional.empty();
        }
        String propertyName = name.getPropertyName();
        AttributeMapping topLevel = attributesByName.get(propertyName);
        if (isPlainScalar(topLevel)) {
            return Optional.of(
                    new Leaf(topLevel.path(), topLevel.binding(), false, topLevel.kind(), topLevel.logicalType()));
        }
        return resolveNestedLeaf(propertyName);
    }

    /** True when {@code attr} is a top-level, non-geometry, non-nested scalar attribute the existing path handles. */
    private static boolean isPlainScalar(AttributeMapping attr) {
        return attr != null && !attr.geometry() && attr.nestedType() == null;
    }

    /** Resolves a nested logical path (slash or dot separated) against the schema to its physical leaf, if present. */
    private Optional<Leaf> resolveNestedLeaf(String propertyName) {
        if (schema == null) {
            return Optional.empty();
        }
        ColumnPath logical = logicalPath(propertyName);
        Optional<ResolvedColumn> resolved = schema.resolve(logical);
        if (resolved.isEmpty()) {
            return Optional.empty();
        }
        ResolvedColumn column = resolved.get();
        Optional<LogicalType> logicalType = logicalTypeOf(column);
        Optional<Class<?>> binding = FeatureTypeMapper.resolveBinding(column.kind(), logicalType);
        return binding.map(b -> new Leaf(column.physical(), b, column.isRepeated(), column.kind(), logicalType));
    }

    /** The logical type annotating the resolved leaf's primitive node, or empty when the node has none. */
    private Optional<LogicalType> logicalTypeOf(ResolvedColumn column) {
        return schema.find(column.physical())
                .filter(SchemaNode.Primitive.class::isInstance)
                .flatMap(node -> ((SchemaNode.Primitive) node).logicalType());
    }

    /** Splits a property name on the ECQL slash form and the dotted programmatic form into a logical column path. */
    private static ColumnPath logicalPath(String propertyName) {
        return ColumnPath.of(propertyName.split("[/.]"));
    }

    /** Maps the GeoTools match action of a multi-valued filter to its parquetry equivalent; defaults to ANY. */
    private static MatchAction matchAction(Filter filter) {
        if (!(filter instanceof MultiValuedFilter multiValued)) {
            return MatchAction.ANY;
        }
        return switch (multiValued.getMatchAction()) {
            case ALL -> MatchAction.ALL;
            case ONE -> MatchAction.ONE;
            case ANY -> MatchAction.ANY;
        };
    }

    private Optional<AttributeMapping> property(Expression e) {
        if (e instanceof PropertyName name) {
            return Optional.ofNullable(attributesByName.get(name.getPropertyName()));
        }
        return Optional.empty();
    }

    private static Optional<Object> literal(Expression e) {
        if (e instanceof Literal lit) {
            return Optional.ofNullable(lit.getValue());
        }
        return Optional.empty();
    }
}
