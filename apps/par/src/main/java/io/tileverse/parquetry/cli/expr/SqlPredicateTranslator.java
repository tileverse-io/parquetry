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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;

/** Translates a parsed SQL condition (jsqlparser AST) into a parquetry {@link Predicate}. */
final class SqlPredicateTranslator {

    private final ParquetSchema schema;
    private final SpatialFilterTranslator spatial;

    SqlPredicateTranslator(ParquetSchema schema, Set<ColumnPath> geometryColumns) {
        this.schema = schema;
        this.spatial = new SpatialFilterTranslator(geometryColumns);
    }

    Predicate translate(Expression expression) {
        return switch (expression) {
            case AndExpression and -> Pred.and(translate(and.getLeftExpression()), translate(and.getRightExpression()));
            case OrExpression or -> Pred.or(translate(or.getLeftExpression()), translate(or.getRightExpression()));
            case NotExpression not -> Pred.not(translate(not.getExpression()));
            case ComparisonOperator comparison -> comparison(comparison);
            case IsNullExpression isNull -> isNull(isNull);
            case InExpression in -> in(in);
            case Between between -> between(between);
            case Function fn -> spatial.translate(fn);
            default -> throw unsupported(describe(expression));
        };
    }

    private Predicate comparison(ComparisonOperator comparison) {
        Column columnNode = requireColumnOnLeft(comparison.getLeftExpression());
        ColumnPath path = pathOf(columnNode);
        Pred.ColumnRef ref = Pred.col(path);
        PrimitiveKind kind = kindOf(path);
        Expression value = comparison.getRightExpression();
        return switch (comparison) {
            case EqualsTo _ -> eq(ref, kind, value);
            case NotEqualsTo _ -> notEq(ref, kind, value);
            case GreaterThan _ -> gt(ref, kind, value);
            case GreaterThanEquals _ -> gtEq(ref, kind, value);
            case MinorThan _ -> lt(ref, kind, value);
            case MinorThanEquals _ -> ltEq(ref, kind, value);
            default -> throw unsupported(describe(comparison));
        };
    }

    private Predicate isNull(IsNullExpression isNull) {
        Column columnNode = requireColumnOnLeft(isNull.getLeftExpression());
        Pred.ColumnRef ref = Pred.col(pathOf(columnNode));
        if (isNull.isNot()) {
            return ref.isNotNull();
        }
        return ref.isNull();
    }

    private Predicate in(InExpression in) {
        Column columnNode = requireColumnOnLeft(in.getLeftExpression());
        ColumnPath path = pathOf(columnNode);
        PrimitiveKind kind = kindOf(path);
        List<Value> values = inValues(in, kind);
        Predicate membership = new Predicate.In(path, values);
        if (in.isNot()) {
            return Pred.not(membership);
        }
        return membership;
    }

    private List<Value> inValues(InExpression in, PrimitiveKind kind) {
        Expression rightSide = in.getRightExpression();
        if (!(rightSide instanceof ExpressionList<?> elements)) {
            throw new FilterParseException("expected a parenthesized value list after IN, got: " + describe(rightSide));
        }
        if (elements.isEmpty()) {
            throw new FilterParseException("IN requires at least one value");
        }
        List<Value> values = new ArrayList<>(elements.size());
        for (Expression element : elements) {
            values.add(value(kind, element));
        }
        return values;
    }

    private Value value(PrimitiveKind kind, Expression element) {
        return switch (kind) {
            case BOOLEAN -> new Value.BoolVal(Literals.asBool(element));
            case INT32 -> new Value.IntVal((int) Literals.asLong(element));
            case INT64 -> new Value.LongVal(Literals.asLong(element));
            case FLOAT -> new Value.FloatVal((float) Literals.asDouble(element));
            case DOUBLE -> new Value.DoubleVal(Literals.asDouble(element));
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> new Value.StringVal(Literals.asString(element));
            default -> throw new FilterParseException("IN not supported for column kind " + kind);
        };
    }

    private Predicate between(Between between) {
        Column columnNode = requireColumnOnLeft(between.getLeftExpression());
        ColumnPath path = pathOf(columnNode);
        Pred.ColumnRef ref = Pred.col(path);
        PrimitiveKind kind = kindOf(path);
        Predicate lowerBound = ordered(OrderedComparison.GT_EQ, ref, kind, between.getBetweenExpressionStart());
        Predicate upperBound = ordered(OrderedComparison.LT_EQ, ref, kind, between.getBetweenExpressionEnd());
        Predicate bounded = Pred.and(lowerBound, upperBound);
        if (between.isNot()) {
            return Pred.not(bounded);
        }
        return bounded;
    }

    private Predicate eq(Pred.ColumnRef ref, PrimitiveKind kind, Expression value) {
        return switch (kind) {
            case BOOLEAN -> ref.eq(Literals.asBool(value));
            case INT32 -> ref.eq((int) Literals.asLong(value));
            case INT64 -> ref.eq(Literals.asLong(value));
            case FLOAT -> ref.eq((float) Literals.asDouble(value));
            case DOUBLE -> ref.eq(Literals.asDouble(value));
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> ref.eq(Literals.asString(value));
            default -> throw unsupportedKind(kind);
        };
    }

    private Predicate notEq(Pred.ColumnRef ref, PrimitiveKind kind, Expression value) {
        return switch (kind) {
            case BOOLEAN -> Pred.not(ref.eq(Literals.asBool(value)));
            case INT32 -> ref.notEq((int) Literals.asLong(value));
            case INT64 -> ref.notEq(Literals.asLong(value));
            // Pred.ColumnRef has no float overload for ordering/notEq; compare FLOAT as double.
            case FLOAT, DOUBLE -> ref.notEq(Literals.asDouble(value));
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> ref.notEq(Literals.asString(value));
            default -> throw unsupportedKind(kind);
        };
    }

    private Predicate lt(Pred.ColumnRef ref, PrimitiveKind kind, Expression value) {
        return ordered(OrderedComparison.LT, ref, kind, value);
    }

    private Predicate ltEq(Pred.ColumnRef ref, PrimitiveKind kind, Expression value) {
        return ordered(OrderedComparison.LT_EQ, ref, kind, value);
    }

    private Predicate gt(Pred.ColumnRef ref, PrimitiveKind kind, Expression value) {
        return ordered(OrderedComparison.GT, ref, kind, value);
    }

    private Predicate gtEq(Pred.ColumnRef ref, PrimitiveKind kind, Expression value) {
        return ordered(OrderedComparison.GT_EQ, ref, kind, value);
    }

    /**
     * Builds an ordered comparison ({@code <}, {@code <=}, {@code >}, {@code >=}) against a numeric column. Shared by
     * the scalar comparison path and by BETWEEN, which lowers to a pair of bounded comparisons; routing both through
     * here keeps literal coercion identical across the two forms.
     */
    private Predicate ordered(OrderedComparison comparison, Pred.ColumnRef ref, PrimitiveKind kind, Expression value) {
        return switch (kind) {
            case INT32 -> comparison.against(ref, (int) Literals.asLong(value));
            case INT64 -> comparison.against(ref, Literals.asLong(value));
            case FLOAT, DOUBLE -> comparison.against(ref, Literals.asDouble(value));
            default -> throw operatorNotSupported(kind);
        };
    }

    private Column requireColumnOnLeft(Expression left) {
        if (left instanceof Column column) {
            return column;
        }
        if (left instanceof Function function) {
            throw unsupported(function.getName());
        }
        throw new FilterParseException("expected a column on the left of the comparison, got: " + describe(left));
    }

    private ColumnPath pathOf(Column column) {
        return ColumnPath.of(column.getFullyQualifiedName().split("\\."));
    }

    private PrimitiveKind kindOf(ColumnPath path) {
        SchemaNode node =
                schema.find(path).orElseThrow(() -> new FilterParseException("no such column: " + path.dot()));
        if (node instanceof SchemaNode.Primitive primitive) {
            return primitive.kind();
        }
        throw new FilterParseException("column is not a leaf: " + path.dot());
    }

    private FilterParseException unsupported(String what) {
        return new FilterParseException("unsupported in --filter: " + what);
    }

    private FilterParseException unsupportedKind(PrimitiveKind kind) {
        return new FilterParseException("unsupported in --filter: comparison on column kind " + kind);
    }

    private FilterParseException operatorNotSupported(PrimitiveKind kind) {
        return new FilterParseException("operator not supported for column kind " + kind);
    }

    private String describe(Expression expression) {
        return expression.getClass().getSimpleName() + ": " + expression;
    }

    /**
     * The four ordered comparisons over numeric columns. Each constant knows how to build the matching predicate for
     * the INT32, INT64, and FLOAT/DOUBLE coercions, keeping the per-kind dispatch in {@link #ordered} free of operator
     * branching.
     */
    private enum OrderedComparison {
        LT {
            @Override
            Predicate against(Pred.ColumnRef ref, int v) {
                return ref.lt(v);
            }

            @Override
            Predicate against(Pred.ColumnRef ref, long v) {
                return ref.lt(v);
            }

            @Override
            Predicate against(Pred.ColumnRef ref, double v) {
                return ref.lt(v);
            }
        },
        LT_EQ {
            @Override
            Predicate against(Pred.ColumnRef ref, int v) {
                return ref.ltEq(v);
            }

            @Override
            Predicate against(Pred.ColumnRef ref, long v) {
                return ref.ltEq(v);
            }

            @Override
            Predicate against(Pred.ColumnRef ref, double v) {
                return ref.ltEq(v);
            }
        },
        GT {
            @Override
            Predicate against(Pred.ColumnRef ref, int v) {
                return ref.gt(v);
            }

            @Override
            Predicate against(Pred.ColumnRef ref, long v) {
                return ref.gt(v);
            }

            @Override
            Predicate against(Pred.ColumnRef ref, double v) {
                return ref.gt(v);
            }
        },
        GT_EQ {
            @Override
            Predicate against(Pred.ColumnRef ref, int v) {
                return ref.gtEq(v);
            }

            @Override
            Predicate against(Pred.ColumnRef ref, long v) {
                return ref.gtEq(v);
            }

            @Override
            Predicate against(Pred.ColumnRef ref, double v) {
                return ref.gtEq(v);
            }
        };

        abstract Predicate against(Pred.ColumnRef ref, int v);

        abstract Predicate against(Pred.ColumnRef ref, long v);

        abstract Predicate against(Pred.ColumnRef ref, double v);
    }
}
