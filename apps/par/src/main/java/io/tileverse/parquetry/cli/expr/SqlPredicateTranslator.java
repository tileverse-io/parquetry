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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import io.tileverse.parquetry.filter.MatchAction;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.ResolvedColumn;
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
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
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
            case ParenthesedExpressionList<?> parenthesized -> translate(groupedCondition(parenthesized));
            case ComparisonOperator comparison -> comparison(comparison);
            case IsNullExpression isNull -> isNull(isNull);
            case InExpression in -> in(in);
            case Between between -> between(between);
            case Function fn -> spatial.translate(fn);
            default -> throw unsupported(describe(expression));
        };
    }

    /** The condition grouped by parentheses; a parenthesized list of several expressions is not a condition. */
    private Expression groupedCondition(ParenthesedExpressionList<?> parenthesized) {
        if (parenthesized.size() != 1) {
            throw unsupported(describe(parenthesized));
        }
        return parenthesized.getFirst();
    }

    private Predicate comparison(ComparisonOperator comparison) {
        Column columnNode = requireColumnOnLeft(comparison.getLeftExpression());
        ResolvedColumn resolved = resolve(columnNode);
        Pred.ColumnRef ref = Pred.col(resolved.physical());
        PrimitiveKind kind = resolved.kind();
        boolean uuid = isUuidColumn(resolved);
        Expression value = comparison.getRightExpression();
        Predicate leaf =
                switch (comparison) {
                    case EqualsTo _ -> eq(ref, kind, uuid, value);
                    case NotEqualsTo _ -> notEq(ref, kind, uuid, value);
                    case GreaterThan _ -> gt(ref, kind, value);
                    case GreaterThanEquals _ -> gtEq(ref, kind, value);
                    case MinorThan _ -> lt(ref, kind, value);
                    case MinorThanEquals _ -> ltEq(ref, kind, value);
                    default -> throw unsupported(describe(comparison));
                };
        return quantifyIfRepeated(resolved, leaf);
    }

    private Predicate isNull(IsNullExpression isNull) {
        Column columnNode = requireColumnOnLeft(isNull.getLeftExpression());
        ResolvedColumn resolved = resolve(columnNode);
        Pred.ColumnRef ref = Pred.col(resolved.physical());
        Predicate leaf = isNull.isNot() ? ref.isNotNull() : ref.isNull();
        return quantifyIfRepeated(resolved, leaf);
    }

    private Predicate in(InExpression in) {
        Column columnNode = requireColumnOnLeft(in.getLeftExpression());
        ResolvedColumn resolved = resolve(columnNode);
        List<Value> values = inValues(in, resolved.kind(), isUuidColumn(resolved));
        Predicate membership = new Predicate.In(resolved.physical(), values);
        Predicate leaf = in.isNot() ? Pred.not(membership) : membership;
        return quantifyIfRepeated(resolved, leaf);
    }

    private List<Value> inValues(InExpression in, PrimitiveKind kind, boolean uuid) {
        Expression rightSide = in.getRightExpression();
        if (!(rightSide instanceof ExpressionList<?> elements)) {
            throw new FilterParseException("expected a parenthesized value list after IN, got: " + describe(rightSide));
        }
        if (elements.isEmpty()) {
            throw new FilterParseException("IN requires at least one value");
        }
        List<Value> values = new ArrayList<>(elements.size());
        for (Expression element : elements) {
            values.add(value(kind, uuid, element));
        }
        return values;
    }

    private Value value(PrimitiveKind kind, boolean uuid, Expression element) {
        return switch (kind) {
            case BOOLEAN -> new Value.BoolVal(Literals.asBool(element));
            case INT32 -> int32Literal(element);
            case INT64 -> new Value.LongVal(Literals.asLong(element));
            case FLOAT -> new Value.FloatVal((float) Literals.asDouble(element));
            case DOUBLE -> new Value.DoubleVal(Literals.asDouble(element));
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY ->
                uuid ? new Value.UuidVal(asUuid(element)) : new Value.StringVal(Literals.asString(element));
            default -> throw new FilterParseException("IN not supported for column kind " + kind);
        };
    }

    private Predicate between(Between between) {
        Column columnNode = requireColumnOnLeft(between.getLeftExpression());
        ResolvedColumn resolved = resolve(columnNode);
        Pred.ColumnRef ref = Pred.col(resolved.physical());
        PrimitiveKind kind = resolved.kind();
        Predicate lowerBound = ordered(OrderedComparison.GT_EQ, ref, kind, between.getBetweenExpressionStart());
        Predicate upperBound = ordered(OrderedComparison.LT_EQ, ref, kind, between.getBetweenExpressionEnd());
        Predicate bounded = Pred.and(lowerBound, upperBound);
        Predicate leaf = between.isNot() ? Pred.not(bounded) : bounded;
        return quantifyIfRepeated(resolved, leaf);
    }

    private Predicate eq(Pred.ColumnRef ref, PrimitiveKind kind, boolean uuid, Expression value) {
        return switch (kind) {
            case BOOLEAN -> ref.eq(Literals.asBool(value));
            case INT32 -> new Predicate.Eq(ref.path(), int32Literal(value));
            case INT64 -> ref.eq(Literals.asLong(value));
            case FLOAT -> ref.eq((float) Literals.asDouble(value));
            case DOUBLE -> ref.eq(Literals.asDouble(value));
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> uuid ? ref.eq(asUuid(value)) : ref.eq(Literals.asString(value));
            default -> throw unsupportedKind(kind);
        };
    }

    private Predicate notEq(Pred.ColumnRef ref, PrimitiveKind kind, boolean uuid, Expression value) {
        return switch (kind) {
            case BOOLEAN -> Pred.not(ref.eq(Literals.asBool(value)));
            case INT32 -> new Predicate.NotEq(ref.path(), int32Literal(value));
            case INT64 -> ref.notEq(Literals.asLong(value));
            // Pred.ColumnRef has no float overload for ordering/notEq; compare FLOAT as double.
            case FLOAT, DOUBLE -> ref.notEq(Literals.asDouble(value));
            // Core has no notEq(UUID); a UUID column lowers to NOT(eq), mirroring the BOOLEAN arm.
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY ->
                uuid ? Pred.not(ref.eq(asUuid(value))) : ref.notEq(Literals.asString(value));
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
     *
     * <p>Ordering on a UUID column is intentionally unsupported: the engine orders {@code FIXED_LEN_BYTE_ARRAY} bytes
     * as unsigned while {@link UUID#compareTo} is signed, which would make a range push incorrect. A UUID column
     * reaches the {@code FIXED_LEN_BYTE_ARRAY} default arm and is rejected like any other binary column.
     */
    private Predicate ordered(OrderedComparison comparison, Pred.ColumnRef ref, PrimitiveKind kind, Expression value) {
        Value literal = orderedLiteral(kind, value);
        return comparison.against(ref.path(), literal);
    }

    /** The literal of an ordered comparison with a column of {@code kind}; a FLOAT column compares as double. */
    private Value orderedLiteral(PrimitiveKind kind, Expression value) {
        return switch (kind) {
            case INT32 -> int32Literal(value);
            case INT64 -> new Value.LongVal(Literals.asLong(value));
            case FLOAT, DOUBLE -> new Value.DoubleVal(Literals.asDouble(value));
            default -> throw operatorNotSupported(kind);
        };
    }

    /**
     * The integer literal of an INT32 column: an int when it fits, a long otherwise. A literal beyond the range of the
     * column narrowed to an int would be another number; the engine compares a long with INT32 cells exactly.
     */
    private static Value int32Literal(Expression literal) {
        long value = Literals.asLong(literal);
        if (value == (int) value) {
            return new Value.IntVal((int) value);
        }
        return new Value.LongVal(value);
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

    /**
     * Resolves a logical column reference to its physical leaf. A logical path addresses fields by their declared names
     * (e.g. {@code addresses.locality}); resolution descends the synthetic LIST {@code list}/{@code element} and MAP
     * {@code key_value} levels the file stores, yielding the physical leaf path and whether it crosses a repeated
     * boundary.
     */
    private ResolvedColumn resolve(Column column) {
        ColumnPath path = ColumnReferences.pathOf(column);
        return schema.resolve(path).orElseThrow(() -> new FilterParseException("no such column: " + path.dot()));
    }

    /**
     * Reports whether the resolved leaf is a Parquet UUID column ({@code FIXED_LEN_BYTE_ARRAY(16)} annotated with
     * {@link LogicalType.UuidType}). A {@link Value.UuidVal} is the only value the normalizer accepts against such a
     * column, which is why the binary comparison arms must coerce a UUID literal to {@code UuidVal} rather than the
     * UTF-8 {@code StringVal} they emit for every other binary column.
     */
    private boolean isUuidColumn(ResolvedColumn resolved) {
        return schema.find(resolved.physical())
                .filter(SchemaNode.Primitive.class::isInstance)
                .flatMap(node -> ((SchemaNode.Primitive) node).logicalType())
                .filter(LogicalType.UuidType.class::isInstance)
                .isPresent();
    }

    private UUID asUuid(Expression value) {
        String text = Literals.asString(value);
        try {
            return UUID.fromString(text);
        } catch (IllegalArgumentException _) {
            throw new FilterParseException("expected a UUID literal, got: " + text);
        }
    }

    /**
     * Wraps a scalar leaf comparison in an {@code ANY} quantifier when the resolved leaf is multi-valued (a list or map
     * element). A single-valued leaf returns its comparison unchanged. Aggregating with {@code ANY} matches the row if
     * any element satisfies the comparison, mirroring SQL's implicit existential quantification over repeated fields.
     */
    private Predicate quantifyIfRepeated(ResolvedColumn resolved, Predicate leaf) {
        if (resolved.isRepeated()) {
            return new Predicate.Quantified(MatchAction.ANY, leaf);
        }
        return leaf;
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
     * The four ordered comparisons over numeric columns. Each constant builds its predicate from a column and a
     * literal, keeping the per-kind coercion in {@link #orderedLiteral} free of operator branching.
     */
    private enum OrderedComparison {
        LT {
            @Override
            Predicate against(ColumnPath column, Value literal) {
                return new Predicate.Lt(column, literal);
            }
        },
        LT_EQ {
            @Override
            Predicate against(ColumnPath column, Value literal) {
                return new Predicate.LtEq(column, literal);
            }
        },
        GT {
            @Override
            Predicate against(ColumnPath column, Value literal) {
                return new Predicate.Gt(column, literal);
            }
        },
        GT_EQ {
            @Override
            Predicate against(ColumnPath column, Value literal) {
                return new Predicate.GtEq(column, literal);
            }
        };

        abstract Predicate against(ColumnPath column, Value literal);
    }
}
