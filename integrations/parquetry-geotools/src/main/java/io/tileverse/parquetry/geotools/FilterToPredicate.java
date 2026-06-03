/*
 * Copyright (c) 2026 Multivers.io
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
package io.tileverse.parquetry.geotools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import org.geotools.api.filter.And;
import org.geotools.api.filter.BinaryComparisonOperator;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.FilterFactory;
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
import org.geotools.api.filter.spatial.BinarySpatialOperator;
import org.geotools.api.geometry.BoundingBox;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.referencing.CRS;
import org.geotools.util.Converters;
import org.locationtech.jts.geom.Geometry;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.geo.jts.JtsGeometryFilter;
import io.tileverse.parquetry.geotools.GeoParquetSchemaMapper.AttributeMapping;
import io.tileverse.parquetry.geotools.GeoParquetSchemaMapper.Mapping;
import io.tileverse.parquetry.schema.ColumnPath;

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
    private final CoordinateReferenceSystem nativeCrs;
    private final FilterFactory ff = CommonFactoryFinder.getFilterFactory();

    FilterToPredicate(Mapping mapping, CoordinateReferenceSystem nativeCrs) {
        Map<String, AttributeMapping> byName = new HashMap<>();
        for (AttributeMapping attr : mapping.attributes()) {
            byName.put(attr.name(), attr);
        }
        this.attributesByName = Map.copyOf(byName);
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
            default ->
                leaf(filter)
                        .map(predicate -> new Result(predicate, Filter.INCLUDE))
                        .orElseGet(() -> new Result(Predicate.ALWAYS_TRUE, filter));
        };
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
        Optional<AttributeMapping> attr = property(f.getExpression1());
        Optional<Object> value = literal(f.getExpression2());
        if (attr.isEmpty() || value.isEmpty()) {
            return Optional.empty();
        }
        return build(attr.get(), op, value.get());
    }

    private Optional<Predicate> between(PropertyIsBetween f) {
        Optional<AttributeMapping> attr = property(f.getExpression());
        Optional<Object> lo = literal(f.getLowerBoundary());
        Optional<Object> hi = literal(f.getUpperBoundary());
        if (attr.isEmpty() || lo.isEmpty() || hi.isEmpty()) {
            return Optional.empty();
        }
        Optional<Predicate> low = build(attr.get(), Op.GTE, lo.get());
        Optional<Predicate> high = build(attr.get(), Op.LTE, hi.get());
        if (low.isEmpty() || high.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(Pred.and(low.get(), high.get()));
    }

    private Optional<Predicate> isNull(PropertyIsNull f) {
        return property(f.getExpression()).map(attr -> Pred.col(attr.path()).isNull());
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

    private Optional<Predicate> build(AttributeMapping attr, Op op, Object rawValue) {
        ColumnPath p = attr.path();
        Class<?> binding = attr.binding();
        if (binding == Integer.class) {
            Integer v = Converters.convert(rawValue, Integer.class);
            if (v == null || !isLossless(rawValue, v.doubleValue())) {
                return Optional.empty();
            }
            return ordered(p, op, v.intValue());
        }
        if (binding == Long.class) {
            Long v = Converters.convert(rawValue, Long.class);
            if (v == null || !isLossless(rawValue, v.doubleValue())) {
                return Optional.empty();
            }
            return ordered(p, op, v.longValue());
        }
        if (binding == Double.class) {
            Double v = Converters.convert(rawValue, Double.class);
            return v == null ? Optional.empty() : ordered(p, op, v.doubleValue());
        }
        if (binding == Float.class) {
            Float v = Converters.convert(rawValue, Float.class);
            if (v == null || op != Op.EQ) {
                return Optional.empty();
            }
            return Optional.of(Pred.col(p).eq(v.floatValue()));
        }
        if (binding == String.class) {
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
        if (binding == Boolean.class) {
            Boolean v = Converters.convert(rawValue, Boolean.class);
            if (v == null || op != Op.EQ) {
                return Optional.empty();
            }
            return Optional.of(Pred.col(p).eq(v.booleanValue()));
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
