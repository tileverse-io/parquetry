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
package io.tileverse.parquetry.cli.expr;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.geo.JtsGeometryFilter;
import io.tileverse.parquetry.schema.ColumnPath;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;

/**
 * Translates an {@code ST_*} SQL function call into a spatial {@link Predicate}.
 *
 * <p>The call names a geometric relation between a geometry column and a query geometry, for example
 * {@code ST_Intersects(geometry, ST_GeomFromText('POLYGON(...)'))}. The query geometry is built from a nested
 * constructor ({@code ST_GeomFromText} or {@code ST_MakeEnvelope}). The relation maps to a {@link JtsGeometryFilter}
 * factory, which evaluates the relation exactly against the file's geometries in their native CRS; no reprojection is
 * performed. Directional relations (Contains/Within, Covers/CoveredBy) invert when the column is the second argument.
 *
 * <p>Wrapping the geometry column in {@code ST_Extent(...)} (synonym {@code ST_Envelope}) asks for the bounding-box
 * relation instead: the call is answered from the geometry's rectangle by {@link BboxRelations}, and the query operand
 * must then be a rectangle as written.
 */
final class SpatialFilterTranslator {

    private static final List<String> EXTENT_FUNCTIONS = List.of("ST_Extent", "ST_Envelope");

    private final Set<ColumnPath> geometryColumns;

    SpatialFilterTranslator(Set<ColumnPath> geometryColumns) {
        this.geometryColumns = geometryColumns;
    }

    Predicate translate(Function function) {
        Relation relation = Relation.bySqlName(function.getName());
        List<Expression> arguments = requireArity(parameters(function), relation.arity(), relation.sqlName());
        Operands operands = resolveOperands(arguments.get(0), arguments.get(1));
        double distance = relation.needsDistance() ? Literals.asDouble(arguments.get(2)) : 0.0;
        return switch (operands.column()) {
            case ExtentColumn extentColumn ->
                BboxRelations.translate(relation, boxCall(extentColumn, operands, distance));
            case BareColumn _ -> Predicate.geometryFilter(relation.build(exactCall(operands, distance)));
        };
    }

    /** The {@code ST_*} relation function names this translator recognizes, in declaration order. */
    static List<String> relationFunctionNames() {
        return Arrays.stream(Relation.values()).map(Relation::sqlName).toList();
    }

    /** The {@code ST_*} query-geometry constructor names this translator recognizes, in declaration order. */
    static List<String> queryConstructorNames() {
        return Arrays.stream(QueryConstructor.values())
                .map(QueryConstructor::sqlName)
                .toList();
    }

    /** The extent function names this translator recognizes, canonical first. */
    static List<String> extentFunctionNames() {
        return EXTENT_FUNCTIONS;
    }

    private SpatialCall exactCall(Operands operands, double distance) {
        return new SpatialCall(operands.column().path(), operands.query().geometry(), operands.columnFirst(), distance);
    }

    /**
     * Builds the bounding-box request, rejecting a query the relation cannot be answered against. A box relation
     * compares two rectangles, and an empty geometry has no rectangle to compare.
     */
    private BboxRelations.BoxCall boxCall(ExtentColumn column, Operands operands, double distance) {
        QueryOperand query = operands.query();
        if (!query.rectangleValued()) {
            throw new FilterParseException("a bounding-box relation needs a rectangle on both sides; wrap the query"
                    + " in ST_Extent(...) to compare bounding boxes, or remove " + column.call()
                    + " to test the exact geometry");
        }
        Envelope envelope = query.geometry().getEnvelopeInternal();
        if (envelope.isNull()) {
            throw new FilterParseException(
                    "the query geometry is empty; a bounding-box relation needs a non-empty rectangle");
        }
        Bbox box = Bbox.of2d(envelope.getMinX(), envelope.getMinY(), envelope.getMaxX(), envelope.getMaxY());
        return new BboxRelations.BoxCall(column.path(), box, operands.columnFirst(), distance, column.call());
    }

    private Operands resolveOperands(Expression first, Expression second) {
        Operand left = classify(first);
        Operand right = classify(second);
        if (left instanceof ColumnOperand column && right instanceof QueryOperand query) {
            boolean columnFirst = true;
            return new Operands(column, query, columnFirst);
        }
        if (left instanceof QueryOperand query && right instanceof ColumnOperand column) {
            boolean columnFirst = false;
            return new Operands(column, query, columnFirst);
        }
        throw new FilterParseException(
                "ST_* requires a geometry column and a query geometry, got: " + first + ", " + second);
    }

    private Operand classify(Expression expression) {
        return switch (expression) {
            case Column column -> new BareColumn(requireGeometryColumn(column));
            case Function function -> functionOperand(function);
            default ->
                throw new FilterParseException(
                        "ST_* requires a geometry column and a query geometry, got: " + expression);
        };
    }

    private Operand functionOperand(Function function) {
        if (isExtentFunction(function.getName())) {
            return extentOperand(function);
        }
        Geometry geometry = queryGeometry(function);
        boolean rectangleValued = isEnvelopeConstructor(function.getName());
        return new QueryOperand(geometry, rectangleValued);
    }

    /** Resolves one extent call. Nested extent calls fold away: the bounding rectangle of a rectangle is itself. */
    private Operand extentOperand(Function extent) {
        List<Expression> single = requireArity(parameters(extent), 1, extent.getName());
        Expression inner = single.get(0);
        return switch (inner) {
            case Column column -> new ExtentColumn(requireGeometryColumn(column, extent.getName()), extent.getName());
            case Function nested when isExtentFunction(nested.getName()) -> extentOperand(nested);
            case Function constructor ->
                new QueryOperand(queryGeometry(constructor).getEnvelope(), true);
            default ->
                throw new FilterParseException(
                        extent.getName() + " takes a geometry column or a query geometry, got: " + inner);
        };
    }

    private ColumnPath requireGeometryColumn(Column column) {
        ColumnPath path = columnPath(column);
        if (!geometryColumns.contains(path)) {
            throw new FilterParseException("ST_* requires a geometry column, got: " + path.dot());
        }
        return path;
    }

    private ColumnPath requireGeometryColumn(Column column, String extentFunction) {
        ColumnPath path = columnPath(column);
        if (!geometryColumns.contains(path)) {
            throw new FilterParseException(extentFunction + " takes a geometry column, got: " + path.dot()
                    + " (the bbox covering columns are used automatically; filter the geometry column)");
        }
        return path;
    }

    private static boolean isExtentFunction(String name) {
        return EXTENT_FUNCTIONS.stream().anyMatch(candidate -> candidate.equalsIgnoreCase(name));
    }

    private static boolean isEnvelopeConstructor(String name) {
        return QueryConstructor.ST_MAKE_ENVELOPE.sqlName().equalsIgnoreCase(name);
    }

    private ColumnPath columnPath(Column column) {
        return ColumnPath.of(column.getFullyQualifiedName().split("\\."));
    }

    private Geometry queryGeometry(Function constructor) {
        QueryConstructor kind = QueryConstructor.bySqlName(constructor.getName());
        return kind.build(parameters(constructor), this);
    }

    private Geometry fromWkt(List<Expression> arguments) {
        List<Expression> single = requireArity(arguments, 1, "ST_GeomFromText");
        String wkt = Literals.asString(single.get(0));
        try {
            return new WKTReader().read(wkt);
        } catch (ParseException _) {
            throw new FilterParseException("invalid WKT: " + wkt);
        }
    }

    private Geometry fromEnvelope(List<Expression> arguments) {
        List<Expression> corners = requireArity(arguments, 4, "ST_MakeEnvelope");
        double minX = Literals.asDouble(corners.get(0));
        double minY = Literals.asDouble(corners.get(1));
        double maxX = Literals.asDouble(corners.get(2));
        double maxY = Literals.asDouble(corners.get(3));
        Envelope envelope = new Envelope(minX, maxX, minY, maxY);
        return new GeometryFactory().toGeometry(envelope);
    }

    private List<Expression> parameters(Function function) {
        if (function.getParameters() == null) {
            return List.of();
        }
        return List.copyOf(function.getParameters());
    }

    private List<Expression> requireArity(List<Expression> arguments, int expected, String name) {
        if (arguments.size() != expected) {
            throw new FilterParseException(name + " expects " + expected + " argument(s), got " + arguments.size());
        }
        return arguments;
    }

    /** The geometry column, the parsed query geometry, the argument order, and any distance for the relation. */
    private record SpatialCall(ColumnPath column, Geometry query, boolean columnFirst, double distance) {}

    /** One side of a spatial relation: the geometry column, or the query geometry. */
    private sealed interface Operand {}

    /** The geometry column side of a relation. The extent-wrapped subtype is what selects the bbox tier. */
    private sealed interface ColumnOperand extends Operand {
        ColumnPath path();
    }

    /** The geometry column as written, tested exactly against the query geometry. */
    private record BareColumn(ColumnPath path) implements ColumnOperand {}

    /**
     * The geometry column reduced to its bounding rectangle by {@code extentFunction}, spelled as the user wrote it.
     */
    private record ExtentColumn(ColumnPath path, String extentFunction) implements ColumnOperand {

        /** Renders the extent call the way it appears in the filter text, for rejection messages. */
        String call() {
            return extentFunction + "(" + path.dot() + ")";
        }
    }

    /**
     * The query geometry, already reduced to its envelope when the call wrapped it in an extent function.
     * {@code rectangleValued} records whether the text asked for a rectangle, either by an extent call or by
     * {@code ST_MakeEnvelope}. It is never inferred from the coordinates: a WKT polygon that happens to be a rectangle
     * still has to be wrapped.
     */
    private record QueryOperand(Geometry geometry, boolean rectangleValued) implements Operand {}

    /** The two operands of a spatial relation, and which side the geometry column was written on. */
    private record Operands(ColumnOperand column, QueryOperand query, boolean columnFirst) {}

    /**
     * The query-geometry constructors recognized as a nested function argument. Each builds a JTS geometry from its
     * literal arguments using the parser's literal coercion and a fresh, single-use JTS reader.
     */
    private enum QueryConstructor {
        ST_GEOM_FROM_TEXT("ST_GeomFromText") {
            @Override
            Geometry build(List<Expression> arguments, SpatialFilterTranslator translator) {
                return translator.fromWkt(arguments);
            }
        },
        ST_MAKE_ENVELOPE("ST_MakeEnvelope") {
            @Override
            Geometry build(List<Expression> arguments, SpatialFilterTranslator translator) {
                return translator.fromEnvelope(arguments);
            }
        };

        private final String sqlName;

        QueryConstructor(String sqlName) {
            this.sqlName = sqlName;
        }

        String sqlName() {
            return sqlName;
        }

        abstract Geometry build(List<Expression> arguments, SpatialFilterTranslator translator);

        static QueryConstructor bySqlName(String name) {
            for (QueryConstructor constructor : values()) {
                if (constructor.sqlName.equalsIgnoreCase(name)) {
                    return constructor;
                }
            }
            throw new FilterParseException("unsupported query geometry constructor: " + name);
        }
    }

    /**
     * The geometric relations exposed as {@code ST_*} SQL functions, each mapped to a {@link JtsGeometryFilter}
     * factory. Symmetric relations build the same filter regardless of argument order; directional relations
     * (Contains/Within, Covers/CoveredBy) invert when the column is the second argument.
     */
    enum Relation {
        ST_INTERSECTS("ST_Intersects") {
            @Override
            JtsGeometryFilter build(SpatialCall call) {
                return JtsGeometryFilter.intersects(call.column(), call.query());
            }
        },
        ST_TOUCHES("ST_Touches") {
            @Override
            JtsGeometryFilter build(SpatialCall call) {
                return JtsGeometryFilter.touches(call.column(), call.query());
            }
        },
        ST_CROSSES("ST_Crosses") {
            @Override
            JtsGeometryFilter build(SpatialCall call) {
                return JtsGeometryFilter.crosses(call.column(), call.query());
            }
        },
        ST_OVERLAPS("ST_Overlaps") {
            @Override
            JtsGeometryFilter build(SpatialCall call) {
                return JtsGeometryFilter.overlaps(call.column(), call.query());
            }
        },
        ST_DISJOINT("ST_Disjoint") {
            @Override
            JtsGeometryFilter build(SpatialCall call) {
                return JtsGeometryFilter.disjoint(call.column(), call.query());
            }
        },
        ST_EQUALS("ST_Equals") {
            @Override
            JtsGeometryFilter build(SpatialCall call) {
                return JtsGeometryFilter.equalsExact(call.column(), call.query());
            }
        },
        ST_CONTAINS("ST_Contains") {
            @Override
            JtsGeometryFilter build(SpatialCall call) {
                if (call.columnFirst()) {
                    return JtsGeometryFilter.contains(call.column(), call.query());
                }
                return JtsGeometryFilter.within(call.column(), call.query());
            }
        },
        ST_WITHIN("ST_Within") {
            @Override
            JtsGeometryFilter build(SpatialCall call) {
                if (call.columnFirst()) {
                    return JtsGeometryFilter.within(call.column(), call.query());
                }
                return JtsGeometryFilter.contains(call.column(), call.query());
            }
        },
        ST_COVERS("ST_Covers") {
            @Override
            JtsGeometryFilter build(SpatialCall call) {
                if (call.columnFirst()) {
                    return JtsGeometryFilter.covers(call.column(), call.query());
                }
                return JtsGeometryFilter.coveredBy(call.column(), call.query());
            }
        },
        ST_COVERED_BY("ST_CoveredBy") {
            @Override
            JtsGeometryFilter build(SpatialCall call) {
                if (call.columnFirst()) {
                    return JtsGeometryFilter.coveredBy(call.column(), call.query());
                }
                return JtsGeometryFilter.covers(call.column(), call.query());
            }
        },
        ST_DWITHIN("ST_DWithin") {
            @Override
            boolean needsDistance() {
                return true;
            }

            @Override
            JtsGeometryFilter build(SpatialCall call) {
                return JtsGeometryFilter.dwithin(call.column(), call.query(), call.distance());
            }
        };

        private final String sqlName;

        Relation(String sqlName) {
            this.sqlName = sqlName;
        }

        abstract JtsGeometryFilter build(SpatialCall call);

        String sqlName() {
            return sqlName;
        }

        boolean needsDistance() {
            return false;
        }

        int arity() {
            if (needsDistance()) {
                return 3;
            }
            return 2;
        }

        static Relation bySqlName(String name) {
            for (Relation relation : values()) {
                if (relation.sqlName.equalsIgnoreCase(name)) {
                    return relation;
                }
            }
            throw new FilterParseException("unsupported ST_* function: " + name);
        }
    }
}
