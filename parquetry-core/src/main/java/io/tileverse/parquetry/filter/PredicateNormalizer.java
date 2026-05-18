/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.filter;

import java.util.ArrayList;
import java.util.List;

import io.tileverse.parquetry.ParquetSchemaException;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Schema;

/**
 * Rewrites a {@link Predicate} into a canonical form expected by the filter pipeline. Applies three structural passes -
 * push {@link Predicate.Not} down to the leaves via De Morgan's laws, constant-fold {@link Predicate.Always} nodes, and
 * flatten nested {@link Predicate.And} / {@link Predicate.Or} - and offers a separate schema-validation pass that
 * throws {@link ParquetSchemaException} for unknown columns or value/type mismatches.
 */
public final class PredicateNormalizer {

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
    public static void validate(Predicate p, Schema schema) {
        switch (p) {
            case Predicate.Always a -> {
                /* no columns to check */
            }
            case Predicate.And a -> a.children().forEach(c -> validate(c, schema));
            case Predicate.Or o -> o.children().forEach(c -> validate(c, schema));
            case Predicate.Not n -> validate(n.child(), schema);
            case Predicate.Eq e -> checkLeafColumn(e.col(), schema, e.v());
            case Predicate.NotEq e -> checkLeafColumn(e.col(), schema, e.v());
            case Predicate.Lt e -> checkLeafColumn(e.col(), schema, e.v());
            case Predicate.LtEq e -> checkLeafColumn(e.col(), schema, e.v());
            case Predicate.Gt e -> checkLeafColumn(e.col(), schema, e.v());
            case Predicate.GtEq e -> checkLeafColumn(e.col(), schema, e.v());
            case Predicate.In in -> {
                Field.Primitive prim = requirePrimitive(in.col(), schema);
                for (Value v : in.values()) {
                    requireCompatible(in.col(), prim.kind(), v);
                }
            }
            case Predicate.IsNull i -> requirePrimitive(i.col(), schema);
            case Predicate.IsNotNull i -> requirePrimitive(i.col(), schema);
            case Predicate.BboxIntersects b -> {
                Field.Primitive prim = requirePrimitive(b.col(), schema);
                if (prim.kind() != PrimitiveKind.BYTE_ARRAY && prim.kind() != PrimitiveKind.FIXED_LEN_BYTE_ARRAY) {
                    throw new ParquetSchemaException("BboxIntersects requires a binary column; got "
                            + b.col().dot() + " of type " + prim.kind());
                }
            }
        }
    }

    /** Convenience: normalize then validate against the schema. Returns the normalized predicate. */
    public static Predicate normalizeAndValidate(Predicate p, Schema schema) {
        Predicate normalized = normalize(p);
        validate(normalized, schema);
        return normalized;
    }

    private static Predicate pushDownNot(Predicate p) {
        return switch (p) {
            case Predicate.Not n -> negate(pushDownNot(n.child()));
            case Predicate.And a ->
                new Predicate.And(a.children().stream()
                        .map(PredicateNormalizer::pushDownNot)
                        .toList());
            case Predicate.Or o ->
                new Predicate.Or(o.children().stream()
                        .map(PredicateNormalizer::pushDownNot)
                        .toList());
            default -> p;
        };
    }

    /**
     * Returns the logical negation of an already-Not-pushed predicate. For comparison atoms (Eq/Lt/...) returns the
     * complementary operator. For {@code In} and {@code BboxIntersects} - which have no simple leaf complement - wraps
     * in a {@code Not}; the pipeline keeps these as outer negations.
     */
    private static Predicate negate(Predicate p) {
        return switch (p) {
            case Predicate.Always a -> new Predicate.Always(!a.value());
            case Predicate.And a ->
                new Predicate.Or(
                        a.children().stream().map(PredicateNormalizer::negate).toList());
            case Predicate.Or o ->
                new Predicate.And(
                        o.children().stream().map(PredicateNormalizer::negate).toList());
            case Predicate.Not n -> n.child();
            case Predicate.Eq e -> new Predicate.NotEq(e.col(), e.v());
            case Predicate.NotEq e -> new Predicate.Eq(e.col(), e.v());
            case Predicate.Lt e -> new Predicate.GtEq(e.col(), e.v());
            case Predicate.LtEq e -> new Predicate.Gt(e.col(), e.v());
            case Predicate.Gt e -> new Predicate.LtEq(e.col(), e.v());
            case Predicate.GtEq e -> new Predicate.Lt(e.col(), e.v());
            case Predicate.IsNull i -> new Predicate.IsNotNull(i.col());
            case Predicate.IsNotNull i -> new Predicate.IsNull(i.col());
            case Predicate.In in -> new Predicate.Not(in);
            case Predicate.BboxIntersects b -> new Predicate.Not(b);
        };
    }

    private static Predicate foldAlways(Predicate p) {
        return switch (p) {
            case Predicate.Not n -> {
                Predicate child = foldAlways(n.child());
                yield child instanceof Predicate.Always a ? new Predicate.Always(!a.value()) : new Predicate.Not(child);
            }
            case Predicate.And a -> foldConnective(a.children(), true);
            case Predicate.Or o -> foldConnective(o.children(), false);
            default -> p;
        };
    }

    private static Predicate foldConnective(List<Predicate> children, boolean isAnd) {
        List<Predicate> kept = new ArrayList<>(children.size());
        for (Predicate raw : children) {
            Predicate folded = foldAlways(raw);
            if (folded instanceof Predicate.Always always) {
                if (always.value() != isAnd) {
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
            case Predicate.And a -> flattenConnective(a.children(), true);
            case Predicate.Or o -> flattenConnective(o.children(), false);
            case Predicate.Not n -> new Predicate.Not(flatten(n.child()));
            default -> p;
        };
    }

    private static Predicate flattenConnective(List<Predicate> children, boolean isAnd) {
        List<Predicate> flat = new ArrayList<>(children.size());
        for (Predicate raw : children) {
            Predicate inner = flatten(raw);
            if (isAnd && inner instanceof Predicate.And nested) {
                flat.addAll(nested.children());
            } else if (!isAnd && inner instanceof Predicate.Or nested) {
                flat.addAll(nested.children());
            } else {
                flat.add(inner);
            }
        }
        return reduceConnective(flat, isAnd);
    }

    private static void checkLeafColumn(ColumnPath path, Schema schema, Value v) {
        Field.Primitive prim = requirePrimitive(path, schema);
        requireCompatible(path, prim.kind(), v);
    }

    private static Field.Primitive requirePrimitive(ColumnPath path, Schema schema) {
        Field f = schema.find(path)
                .orElseThrow(() ->
                        new ParquetSchemaException("Column " + path.dot() + " is not defined in the file schema"));
        if (!(f instanceof Field.Primitive p)) {
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
            case Value.BoolVal __ -> kind == PrimitiveKind.BOOLEAN;
            case Value.IntVal __ -> kind == PrimitiveKind.INT32;
            case Value.LongVal __ -> kind == PrimitiveKind.INT64;
            case Value.FloatVal __ -> kind == PrimitiveKind.FLOAT;
            case Value.DoubleVal __ -> kind == PrimitiveKind.DOUBLE;
            case Value.StringVal __ -> kind == PrimitiveKind.BYTE_ARRAY;
            case Value.BinaryVal __ -> kind == PrimitiveKind.BYTE_ARRAY || kind == PrimitiveKind.FIXED_LEN_BYTE_ARRAY;
            case Value.DateVal __ -> kind == PrimitiveKind.INT32;
            case Value.TimestampVal __ -> kind == PrimitiveKind.INT64 || kind == PrimitiveKind.INT96;
        };
    }
}
