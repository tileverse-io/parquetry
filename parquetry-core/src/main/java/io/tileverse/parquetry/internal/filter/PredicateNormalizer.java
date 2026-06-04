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
package io.tileverse.parquetry.internal.filter;

import java.util.ArrayList;
import java.util.List;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ParquetSchemaException;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Rewrites a {@link Predicate} into a canonical form expected by the filter pipeline. Applies three structural passes -
 * push {@link Predicate.Not} down to the leaves via De Morgan's laws, constant-fold {@link Predicate.Always} nodes, and
 * flatten nested {@link Predicate.And} / {@link Predicate.Or} - and offers a separate schema-validation pass that
 * throws {@link ParquetSchemaException} for unknown columns or value/type mismatches.
 */
final class PredicateNormalizer {

    private PredicateNormalizer() {}

    /**
     * Returns a structurally-normalized copy of {@code p}: {@code Not} pushed to leaves, {@code Always} folded, nested
     * {@code And}/{@code Or} flattened, single-child connectives unwrapped, empty connectives collapsed to
     * {@link Predicate#ALWAYS_TRUE} (for {@code And}) or {@link Predicate#ALWAYS_FALSE} (for {@code Or}).
     */
    public static Predicate normalize(Predicate p) {
        return flatten(foldAlways(pushDownNot(p)));
    }

    /**
     * Throws {@link ParquetSchemaException} if {@code p} references a column not in {@code schema}, a group column, or
     * a value type incompatible with the column's primitive kind.
     */
    // S7475 (bare _ in nested record patterns) is informational only - palantirJavaFormat 2.90 cannot
    // parse the bare-underscore form Sonar suggests; see memory feedback-palantir-unnamed-pattern.
    @SuppressWarnings("java:S7475")
    public static void validate(Predicate p, ParquetSchema schema) {
        switch (p) {
            case Predicate.Always _ -> {
                /* no columns to check */
            }
            case Predicate.And(List<Predicate> children) -> children.forEach(c -> validate(c, schema));
            case Predicate.Or(List<Predicate> children) -> children.forEach(c -> validate(c, schema));
            case Predicate.Not(Predicate child) -> validate(child, schema);
            case Predicate.Eq(ColumnPath col, Value v) -> checkLeafColumn(col, schema, v);
            case Predicate.NotEq(ColumnPath col, Value v) -> checkLeafColumn(col, schema, v);
            case Predicate.Lt(ColumnPath col, Value v) -> checkLeafColumn(col, schema, v);
            case Predicate.LtEq(ColumnPath col, Value v) -> checkLeafColumn(col, schema, v);
            case Predicate.Gt(ColumnPath col, Value v) -> checkLeafColumn(col, schema, v);
            case Predicate.GtEq(ColumnPath col, Value v) -> checkLeafColumn(col, schema, v);
            case Predicate.In(ColumnPath col, List<Value> values) -> {
                SchemaNode.Primitive prim = requirePrimitive(col, schema);
                for (Value v : values) {
                    requireCompatible(col, prim.kind(), v);
                }
            }
            case Predicate.IsNull(ColumnPath col) -> requirePrimitive(col, schema);
            case Predicate.IsNotNull(ColumnPath col) -> requirePrimitive(col, schema);
            case Predicate.Spatial s -> {
                SchemaNode.Primitive prim = requirePrimitive(s.col(), schema);
                if (prim.kind() != PrimitiveKind.BYTE_ARRAY && prim.kind() != PrimitiveKind.FIXED_LEN_BYTE_ARRAY) {
                    throw new ParquetSchemaException("a spatial predicate requires a binary column; got "
                            + s.col().dot() + " of type " + prim.kind());
                }
            }
            case Predicate.GeometryFilterPredicate _ -> {
                /* opaque geometry leaf; column validated by the filter */
            }
        }
    }

    /** Convenience: normalize then validate against the schema. Returns the normalized predicate. */
    public static Predicate normalizeAndValidate(Predicate p, ParquetSchema schema) {
        Predicate normalized = normalize(p);
        validate(normalized, schema);
        return normalized;
    }

    private static Predicate pushDownNot(Predicate p) {
        return switch (p) {
            case Predicate.Not(Predicate child) -> negate(pushDownNot(child));
            case Predicate.And(List<Predicate> children) ->
                new Predicate.And(
                        children.stream().map(PredicateNormalizer::pushDownNot).toList());
            case Predicate.Or(List<Predicate> children) ->
                new Predicate.Or(
                        children.stream().map(PredicateNormalizer::pushDownNot).toList());
            default -> p;
        };
    }

    /**
     * Returns the logical negation of an already-Not-pushed predicate. For comparison atoms (Eq/Lt/...) returns the
     * complementary operator. For {@code In} and the spatial relations - which have no simple leaf complement - wraps
     * in a {@code Not}; the pipeline keeps these as outer negations.
     */
    private static Predicate negate(Predicate p) {
        return switch (p) {
            case Predicate.Always(boolean value) -> new Predicate.Always(!value);
            case Predicate.And(List<Predicate> children) ->
                new Predicate.Or(
                        children.stream().map(PredicateNormalizer::negate).toList());
            case Predicate.Or(List<Predicate> children) ->
                new Predicate.And(
                        children.stream().map(PredicateNormalizer::negate).toList());
            case Predicate.Not(Predicate child) -> child;
            case Predicate.Eq(ColumnPath col, Value v) -> new Predicate.NotEq(col, v);
            case Predicate.NotEq(ColumnPath col, Value v) -> new Predicate.Eq(col, v);
            case Predicate.Lt(ColumnPath col, Value v) -> new Predicate.GtEq(col, v);
            case Predicate.LtEq(ColumnPath col, Value v) -> new Predicate.Gt(col, v);
            case Predicate.Gt(ColumnPath col, Value v) -> new Predicate.LtEq(col, v);
            case Predicate.GtEq(ColumnPath col, Value v) -> new Predicate.Lt(col, v);
            case Predicate.IsNull(ColumnPath col) -> new Predicate.IsNotNull(col);
            case Predicate.IsNotNull(ColumnPath col) -> new Predicate.IsNull(col);
            case Predicate.In _ -> new Predicate.Not(p);
            case Predicate.Spatial _ -> new Predicate.Not(p);
            case Predicate.GeometryFilterPredicate _ -> new Predicate.Not(p);
        };
    }

    private static Predicate foldAlways(Predicate p) {
        return switch (p) {
            case Predicate.Not(Predicate inner) -> {
                Predicate folded = foldAlways(inner);
                yield folded instanceof Predicate.Always(boolean value)
                        ? new Predicate.Always(!value)
                        : new Predicate.Not(folded);
            }
            case Predicate.And(List<Predicate> children) -> foldConnective(children, true);
            case Predicate.Or(List<Predicate> children) -> foldConnective(children, false);
            default -> p;
        };
    }

    private static Predicate foldConnective(List<Predicate> children, boolean isAnd) {
        List<Predicate> kept = new ArrayList<>(children.size());
        for (Predicate raw : children) {
            Predicate folded = foldAlways(raw);
            if (folded instanceof Predicate.Always(boolean value)) {
                if (value != isAnd) {
                    return new Predicate.Always(!isAnd);
                }
            } else {
                kept.add(folded);
            }
        }
        return reduceConnective(kept, isAnd);
    }

    private static Predicate reduceConnective(List<Predicate> kept, boolean isAnd) {
        return switch (kept.size()) {
            case 0 -> new Predicate.Always(isAnd);
            case 1 -> kept.get(0);
            default -> isAnd ? new Predicate.And(kept) : new Predicate.Or(kept);
        };
    }

    private static Predicate flatten(Predicate p) {
        return switch (p) {
            case Predicate.And(List<Predicate> children) -> flattenConnective(children, true);
            case Predicate.Or(List<Predicate> children) -> flattenConnective(children, false);
            case Predicate.Not(Predicate child) -> new Predicate.Not(flatten(child));
            default -> p;
        };
    }

    private static Predicate flattenConnective(List<Predicate> children, boolean isAnd) {
        List<Predicate> flat = new ArrayList<>(children.size());
        for (Predicate raw : children) {
            Predicate inner = flatten(raw);
            if (isAnd && inner instanceof Predicate.And(List<Predicate> nested)) {
                flat.addAll(nested);
            } else if (!isAnd && inner instanceof Predicate.Or(List<Predicate> nested)) {
                flat.addAll(nested);
            } else {
                flat.add(inner);
            }
        }
        return reduceConnective(flat, isAnd);
    }

    private static void checkLeafColumn(ColumnPath path, ParquetSchema schema, Value v) {
        SchemaNode.Primitive prim = requirePrimitive(path, schema);
        requireCompatible(path, prim.kind(), v);
    }

    private static SchemaNode.Primitive requirePrimitive(ColumnPath path, ParquetSchema schema) {
        SchemaNode f = schema.find(path)
                .orElseThrow(() ->
                        new ParquetSchemaException("Column " + path.dot() + " is not defined in the file schema"));
        if (!(f instanceof SchemaNode.Primitive p)) {
            throw new ParquetSchemaException("Column " + path.dot() + " is a group, not a primitive column");
        }
        return p;
    }

    private static void requireCompatible(ColumnPath path, PrimitiveKind kind, Value v) {
        if (!isCompatible(kind, v)) {
            throw new ParquetSchemaException("Value of type " + v.getClass().getSimpleName()
                    + " is not compatible with column " + path.dot() + " of type " + kind);
        }
    }

    private static boolean isCompatible(PrimitiveKind kind, Value v) {
        return switch (v) {
            case Value.BoolVal _ -> kind == PrimitiveKind.BOOLEAN;
            case Value.IntVal _ -> kind == PrimitiveKind.INT32;
            case Value.LongVal _ -> kind == PrimitiveKind.INT64;
            case Value.FloatVal _ -> kind == PrimitiveKind.FLOAT;
            case Value.DoubleVal _ -> kind == PrimitiveKind.DOUBLE;
            case Value.StringVal _ -> kind == PrimitiveKind.BYTE_ARRAY;
            case Value.BinaryVal _ -> kind == PrimitiveKind.BYTE_ARRAY || kind == PrimitiveKind.FIXED_LEN_BYTE_ARRAY;
            case Value.DateVal _ -> kind == PrimitiveKind.INT32;
            case Value.TimestampVal _ -> kind == PrimitiveKind.INT64 || kind == PrimitiveKind.INT96;
        };
    }
}
