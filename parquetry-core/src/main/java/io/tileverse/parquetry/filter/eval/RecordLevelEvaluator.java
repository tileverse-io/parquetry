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
package io.tileverse.parquetry.filter.eval;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Tier-5 (RECORD_LEVEL) evaluator. Tests a single assembled record against a normalized {@link Predicate}, returning
 * {@code true} if the record passes.
 *
 * <p>NULL comparisons follow SQL WHERE semantics collapsed to boolean: a value comparison against NULL evaluates to
 * {@code false} (not NULL). {@link Predicate.IsNull} and {@link Predicate.IsNotNull} are the only predicates whose
 * truth value depends on null presence.
 *
 * <p>Assumes the predicate has been normalized (Not pushed to leaves, Always folded, And/Or flattened).
 */
public final class RecordLevelEvaluator {

    private RecordLevelEvaluator() {}

    /** Returns {@code true} if {@code row} satisfies {@code predicate}. */
    public static boolean test(Predicate predicate, RecordAccessor row) {
        return switch (predicate) {
            case Predicate.Always(boolean value) -> value;
            case Predicate.And(List<Predicate> children) -> testAnd(children, row);
            case Predicate.Or(List<Predicate> children) -> testOr(children, row);
            case Predicate.Not(Predicate child) -> !test(child, row);
            case Predicate.Eq(ColumnPath col, Value v) -> compare(row.value(col), v) == 0;
            case Predicate.NotEq(ColumnPath col, Value v) -> {
                Object got = row.value(col);
                yield got != null && compare(got, v) != 0;
            }
            case Predicate.Lt(ColumnPath col, Value v) -> {
                Object got = row.value(col);
                yield got != null && compare(got, v) < 0;
            }
            case Predicate.LtEq(ColumnPath col, Value v) -> {
                Object got = row.value(col);
                yield got != null && compare(got, v) <= 0;
            }
            case Predicate.Gt(ColumnPath col, Value v) -> {
                Object got = row.value(col);
                yield got != null && compare(got, v) > 0;
            }
            case Predicate.GtEq(ColumnPath col, Value v) -> {
                Object got = row.value(col);
                yield got != null && compare(got, v) >= 0;
            }
            case Predicate.In(ColumnPath col, List<Value> values) -> {
                Object got = row.value(col);
                yield got != null && values.stream().anyMatch(v -> compare(got, v) == 0);
            }
            case Predicate.IsNull(ColumnPath col) -> row.value(col) == null;
            case Predicate.IsNotNull(ColumnPath col) -> row.value(col) != null;
            case Predicate.BboxIntersects _ -> false; // not supported at record-level yet (future geometry work)
        };
    }

    private static boolean testAnd(List<Predicate> children, RecordAccessor row) {
        for (Predicate child : children) {
            if (!test(child, row)) {
                return false;
            }
        }
        return true;
    }

    private static boolean testOr(List<Predicate> children, RecordAccessor row) {
        for (Predicate child : children) {
            if (test(child, row)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Compares an actual record value (boxed) against a predicate-side {@link Value}. Returns 0 for unknown type
     * combinations; the surrounding evaluator treats that as equality, but with NULL filtered upstream the typical path
     * is well-typed.
     */
    // S7475 (bare _ in nested record patterns) is informational only - palantirJavaFormat 2.90 cannot
    // parse the bare-underscore form Sonar suggests; see memory feedback-palantir-unnamed-pattern.
    @SuppressWarnings({"java:S3776", "java:S7475"})
    private static int compare(Object actual, Value bound) {
        if (actual == null) {
            return -1; // any comparison vs null returns "less than", but the caller already short-circuited on null
        }
        return switch (bound) {
            case Value.BoolVal(boolean bv) when actual instanceof Boolean av -> Boolean.compare(av, bv);
            case Value.IntVal(int bv) when actual instanceof Integer av -> Integer.compare(av, bv);
            case Value.LongVal(long bv) when actual instanceof Long av -> Long.compare(av, bv);
            case Value.FloatVal(float bv) when actual instanceof Float av -> Float.compare(av, bv);
            case Value.DoubleVal(double bv) when actual instanceof Double av -> Double.compare(av, bv);
            case Value.StringVal(String bv) when actual instanceof String av -> av.compareTo(bv);
            case Value.StringVal(String bv)
            when actual instanceof ByteBuffer av ->
                compareBytes(av, ByteBuffer.wrap(bv.getBytes(StandardCharsets.UTF_8)));
            case Value.BinaryVal(ByteBuffer bv) when actual instanceof ByteBuffer av -> compareBytes(av, bv);
            case Value.DateVal(LocalDate bv) when actual instanceof LocalDate av -> av.compareTo(bv);
            case Value.DateVal(LocalDate bv)
            when actual instanceof Integer av -> Integer.compare(av, (int) bv.toEpochDay());
            case Value.TimestampVal(LocalDateTime bv, boolean _)
            when actual instanceof LocalDateTime av -> av.compareTo(bv);
            default -> 0;
        };
    }

    private static int compareBytes(ByteBuffer a, ByteBuffer b) {
        ByteBuffer ad = a.duplicate();
        ByteBuffer bd = b.duplicate();
        int aLen = ad.remaining();
        int bLen = bd.remaining();
        int common = Math.min(aLen, bLen);
        for (int i = 0; i < common; i++) {
            int diff = (ad.get(ad.position() + i) & 0xff) - (bd.get(bd.position() + i) & 0xff);
            if (diff != 0) {
                return diff;
            }
        }
        return Integer.compare(aLen, bLen);
    }
}
