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
package io.tileverse.parquetry.cli.expr;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.geo.jts.JtsGeometryFilter;
import io.tileverse.parquetry.schema.ColumnPath;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;

/**
 * Translates an {@code ST_*} SQL function call into an exact geometry {@link Predicate}.
 *
 * <p>The call names a geometric relation between a geometry column and a query geometry, for example
 * {@code ST_Intersects(geometry, ST_GeomFromText('POLYGON(...)'))}. The query geometry is built from a nested
 * constructor ({@code ST_GeomFromText} or {@code ST_MakeEnvelope}). The relation maps to a {@link JtsGeometryFilter}
 * factory, which evaluates the relation exactly against the file's geometries in their native CRS; no reprojection is
 * performed. Directional relations (Contains/Within, Covers/CoveredBy) invert when the column is the second argument.
 */
final class SpatialFilterTranslator {

    private final Set<ColumnPath> geometryColumns;

    SpatialFilterTranslator(Set<ColumnPath> geometryColumns) {
        this.geometryColumns = geometryColumns;
    }

    Predicate translate(Function function) {
        Relation relation = Relation.bySqlName(function.getName());
        SpatialCall call = parseCall(function, relation);
        JtsGeometryFilter filter = relation.build(call);
        return Predicate.geometryFilter(filter);
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

    private SpatialCall parseCall(Function function, Relation relation) {
        List<Expression> arguments = requireArity(parameters(function), relation.arity(), relation.sqlName());
        Operands operands = resolveOperands(arguments.get(0), arguments.get(1));
        double distance = relation.needsDistance() ? Literals.asDouble(arguments.get(2)) : 0.0;
        return new SpatialCall(operands.column(), operands.query(), operands.columnFirst(), distance);
    }

    private Operands resolveOperands(Expression first, Expression second) {
        if (isGeometryColumn(first) && isFunctionCall(second)) {
            boolean columnFirst = true;
            return new Operands(columnPath(first), queryGeometry((Function) second), columnFirst);
        }
        if (isFunctionCall(first) && isGeometryColumn(second)) {
            boolean columnFirst = false;
            return new Operands(columnPath(second), queryGeometry((Function) first), columnFirst);
        }
        rejectNonGeometryColumn(first);
        rejectNonGeometryColumn(second);
        throw new FilterParseException(
                "ST_* requires a geometry column and a query geometry, got: " + first + ", " + second);
    }

    private void rejectNonGeometryColumn(Expression expression) {
        if (expression instanceof Column column && !isGeometryColumn(column)) {
            throw new FilterParseException("ST_* requires a geometry column, got: "
                    + columnPath(column).dot());
        }
    }

    private boolean isGeometryColumn(Expression expression) {
        return expression instanceof Column column && isGeometryColumn(column);
    }

    private boolean isGeometryColumn(Column column) {
        return geometryColumns.contains(columnPath(column));
    }

    private boolean isFunctionCall(Expression expression) {
        return expression instanceof Function;
    }

    private ColumnPath columnPath(Expression expression) {
        return columnPath((Column) expression);
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

    /** The two operands of a spatial relation: which column it tests and against which query geometry. */
    private record Operands(ColumnPath column, Geometry query, boolean columnFirst) {}

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
    private enum Relation {
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
