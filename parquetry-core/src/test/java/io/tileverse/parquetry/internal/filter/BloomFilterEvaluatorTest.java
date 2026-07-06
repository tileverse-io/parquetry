/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import static io.tileverse.parquetry.filter.Pred.col;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;

class BloomFilterEvaluatorTest {

    private static final int BYTES_PER_BLOCK = 32;
    private static final int[] SALT = {
        0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31
    };

    @Test
    void eqIntValueAbsentFromBloomIsEliminated() {
        FilterPipeline.BloomFilterLookup blooms =
                single("year", PrimitiveKind.INT32, bloomOver(SplitBlockBloomFilter.hashInt32(2020)));
        Predicate p = col("year").eq(2099);
        assertThat(BloomFilterEvaluator.evaluate(p, blooms)).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void eqIntValuePresentInBloomIsInconclusive() {
        FilterPipeline.BloomFilterLookup blooms =
                single("year", PrimitiveKind.INT32, bloomOver(SplitBlockBloomFilter.hashInt32(2020)));
        Predicate p = col("year").eq(2020);
        assertThat(BloomFilterEvaluator.evaluate(p, blooms)).isInstanceOf(PruningDecision.Inconclusive.class);
    }

    @Test
    void eqWithoutAnyBloomIsNotApplied() {
        FilterPipeline.BloomFilterLookup blooms = empty();
        assertThat(BloomFilterEvaluator.evaluate(col("year").eq(2020), blooms))
                .isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void eqStringAbsentIsEliminated() {
        long h = SplitBlockBloomFilter.hashBytes("Hello".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        FilterPipeline.BloomFilterLookup blooms = single("name", PrimitiveKind.BYTE_ARRAY, bloomOver(h));
        assertThat(BloomFilterEvaluator.evaluate(col("name").eq("absent"), blooms))
                .isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void inAllAbsentValuesEliminates() {
        long h = SplitBlockBloomFilter.hashInt32(7);
        FilterPipeline.BloomFilterLookup blooms = single("status", PrimitiveKind.INT32, bloomOver(h));
        Predicate p = col("status").inInts(1, 2, 3);
        assertThat(BloomFilterEvaluator.evaluate(p, blooms)).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void inOneValuePresentIsInconclusive() {
        long h = SplitBlockBloomFilter.hashInt32(2);
        FilterPipeline.BloomFilterLookup blooms = single("status", PrimitiveKind.INT32, bloomOver(h));
        Predicate p = col("status").inInts(1, 2, 3);
        assertThat(BloomFilterEvaluator.evaluate(p, blooms)).isInstanceOf(PruningDecision.Inconclusive.class);
    }

    @Test
    void rangePredicateIsNotApplied() {
        long h = SplitBlockBloomFilter.hashInt32(2020);
        FilterPipeline.BloomFilterLookup blooms = single("year", PrimitiveKind.INT32, bloomOver(h));
        // Range predicates have no bloom-filter semantics.
        assertThat(BloomFilterEvaluator.evaluate(col("year").lt(2010), blooms))
                .isInstanceOf(PruningDecision.NotApplied.class);
        assertThat(BloomFilterEvaluator.evaluate(col("year").gt(2030), blooms))
                .isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void mismatchedValueKindIsNotApplied() {
        long h = SplitBlockBloomFilter.hashInt32(2020);
        FilterPipeline.BloomFilterLookup blooms = single("year", PrimitiveKind.INT32, bloomOver(h));
        // String-valued predicate against an INT32 column - we can't compute a matching hash, so degrade gracefully.
        assertThat(BloomFilterEvaluator.evaluate(col("year").eq("nope"), blooms))
                .isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void andEliminatedChildShortCircuits() {
        long present = SplitBlockBloomFilter.hashInt32(1);
        FilterPipeline.BloomFilterLookup blooms = twoColumn(
                "status",
                PrimitiveKind.INT32,
                bloomOver(present),
                "year",
                PrimitiveKind.INT32,
                bloomOver(SplitBlockBloomFilter.hashInt32(2000)));
        // year == 9999 is absent -> child eliminated -> AND eliminated.
        Predicate p = col("status").eq(1).and(col("year").eq(9999));
        assertThat(BloomFilterEvaluator.evaluate(p, blooms)).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void orRequiresEveryChildEliminated() {
        long present = SplitBlockBloomFilter.hashInt32(1);
        FilterPipeline.BloomFilterLookup blooms = twoColumn(
                "status",
                PrimitiveKind.INT32,
                bloomOver(present),
                "year",
                PrimitiveKind.INT32,
                bloomOver(SplitBlockBloomFilter.hashInt32(2000)));
        // status == 1 might be present -> OR cannot be eliminated even if the other branch is eliminated.
        Predicate p = col("status").eq(1).or(col("year").eq(9999));
        assertThat(BloomFilterEvaluator.evaluate(p, blooms)).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void alwaysFalseIsEliminated() {
        assertThat(BloomFilterEvaluator.evaluate(Predicate.ALWAYS_FALSE, empty()))
                .isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void alwaysTrueIsNotApplied() {
        assertThat(BloomFilterEvaluator.evaluate(Predicate.ALWAYS_TRUE, empty()))
                .isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void notEqIsNotApplied() {
        FilterPipeline.BloomFilterLookup blooms =
                single("year", PrimitiveKind.INT32, bloomOver(SplitBlockBloomFilter.hashInt32(2020)));
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("year").notEq(2020), blooms))
                .isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void rangeAndNullPredicatesAreNotApplied() {
        // Cover the remaining operator branches that have no bloom semantics.
        FilterPipeline.BloomFilterLookup blooms =
                single("year", PrimitiveKind.INT32, bloomOver(SplitBlockBloomFilter.hashInt32(2020)));
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("year").ltEq(2020), blooms))
                .isInstanceOf(PruningDecision.NotApplied.class);
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("year").gtEq(2020), blooms))
                .isInstanceOf(PruningDecision.NotApplied.class);
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("year").isNull(), blooms))
                .isInstanceOf(PruningDecision.NotApplied.class);
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("year").isNotNull(), blooms))
                .isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void notLeafIsNotApplied() {
        // NOT cannot be turned into a bloom-tier elimination - even if the child predicate would be eliminated, that
        // doesn't prove the negation can't be satisfied (false positives in the bloom mean the negation might still
        // match some rows). We always degrade gracefully.
        FilterPipeline.BloomFilterLookup blooms =
                single("year", PrimitiveKind.INT32, bloomOver(SplitBlockBloomFilter.hashInt32(2020)));
        assertThat(BloomFilterEvaluator.evaluate(Pred.not(Pred.col("year").eq(9999)), blooms))
                .isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void longAbsentValueIsEliminated() {
        long present = SplitBlockBloomFilter.hashInt64(1_000_000_000_000L);
        FilterPipeline.BloomFilterLookup blooms = single("id", PrimitiveKind.INT64, bloomOver(present));
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("id").eq(42L), blooms))
                .isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void floatAbsentValueIsEliminated() {
        float present = 1.5f;
        FilterPipeline.BloomFilterLookup blooms =
                single("ratio", PrimitiveKind.FLOAT, bloomOver(SplitBlockBloomFilter.hashFloat(present)));
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("ratio").eq(99.0f), blooms))
                .isInstanceOf(PruningDecision.Eliminated.class);
        // Same probe value passes through as Inconclusive (the bloom was consulted, value may be present).
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("ratio").eq(present), blooms))
                .isInstanceOf(PruningDecision.Inconclusive.class);
    }

    @Test
    void doubleAbsentValueIsEliminated() {
        double present = 3.14;
        FilterPipeline.BloomFilterLookup blooms =
                single("ratio", PrimitiveKind.DOUBLE, bloomOver(SplitBlockBloomFilter.hashDouble(present)));
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("ratio").eq(99.0), blooms))
                .isInstanceOf(PruningDecision.Eliminated.class);
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("ratio").eq(present), blooms))
                .isInstanceOf(PruningDecision.Inconclusive.class);
    }

    @Test
    void inListWithNonHashableValueIsNotApplied() {
        // String-valued IN-list against an INT32 column - the evaluator can't hash a string for an INT32 bloom, so
        // every list value contributes a NotApplied and the In itself surfaces as NotApplied.
        FilterPipeline.BloomFilterLookup blooms =
                single("year", PrimitiveKind.INT32, bloomOver(SplitBlockBloomFilter.hashInt32(2020)));
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("year").inStrings("not-an-int"), blooms))
                .isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void bboxIntersectsIsNotApplied() {
        FilterPipeline.BloomFilterLookup blooms = empty();
        assertThat(BloomFilterEvaluator.evaluate(
                        Pred.col("geom").intersects(io.tileverse.parquetry.filter.Bbox.of2d(0, 0, 1, 1)), blooms))
                .isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void boolValueAgainstAnyColumnKindIsNotApplied() {
        // Bool predicates have no bloom semantics defined by Parquet (no canonical "plain encoding" hash for booleans
        // in the spec). The evaluator must surface that as NotApplied regardless of the column kind.
        FilterPipeline.BloomFilterLookup blooms =
                single("flag", PrimitiveKind.BOOLEAN, bloomOver(SplitBlockBloomFilter.hashInt32(1)));
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("flag").eq(true), blooms))
                .isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void intValueAgainstWrongColumnKindIsNotApplied() {
        // INT32 predicate value but the column is INT64 - the hash widths don't match, so we can't compute a probe.
        FilterPipeline.BloomFilterLookup blooms =
                single("id", PrimitiveKind.INT64, bloomOver(SplitBlockBloomFilter.hashInt64(42L)));
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("id").eq(42), blooms))
                .isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void longValueAgainstWrongColumnKindIsNotApplied() {
        FilterPipeline.BloomFilterLookup blooms =
                single("year", PrimitiveKind.INT32, bloomOver(SplitBlockBloomFilter.hashInt32(2020)));
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("year").eq(2020L), blooms))
                .isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void doubleValueAgainstFloatColumnIsNotApplied() {
        FilterPipeline.BloomFilterLookup blooms =
                single("ratio", PrimitiveKind.FLOAT, bloomOver(SplitBlockBloomFilter.hashFloat(1.5f)));
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("ratio").eq(1.5), blooms))
                .isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void binaryValueAgainstByteArrayColumnIsHashed() {
        byte[] bytes = "Hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        FilterPipeline.BloomFilterLookup blooms =
                single("blob", PrimitiveKind.BYTE_ARRAY, bloomOver(SplitBlockBloomFilter.hashBytes(bytes)));
        // Present binary -> Inconclusive (might match).
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("blob").eq(bytes), blooms))
                .isInstanceOf(PruningDecision.Inconclusive.class);
        // Absent binary -> Eliminated.
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("blob").eq("not-here".getBytes()), blooms))
                .isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void binaryValueAgainstFixedLenByteArrayColumnIsHashed() {
        byte[] bytes = {1, 2, 3, 4};
        FilterPipeline.BloomFilterLookup blooms =
                single("fixed", PrimitiveKind.FIXED_LEN_BYTE_ARRAY, bloomOver(SplitBlockBloomFilter.hashBytes(bytes)));
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("fixed").eq(bytes), blooms))
                .isInstanceOf(PruningDecision.Inconclusive.class);
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("fixed").eq(new byte[] {9, 9, 9, 9}), blooms))
                .isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void binaryValueAgainstIntColumnIsNotApplied() {
        // BinaryVal predicate against a numeric column - no matching hash; degrade gracefully.
        FilterPipeline.BloomFilterLookup blooms =
                single("year", PrimitiveKind.INT32, bloomOver(SplitBlockBloomFilter.hashInt32(2020)));
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("year").eq(new byte[] {1, 2}), blooms))
                .isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void dateValueAgainstInt32ColumnIsHashed() {
        // Parquet plain-encoding for DATE columns is int32 days-since-epoch. The evaluator hashes the epoch-day int.
        java.time.LocalDate date = java.time.LocalDate.of(2020, 1, 1);
        long h = SplitBlockBloomFilter.hashInt32((int) date.toEpochDay());
        FilterPipeline.BloomFilterLookup blooms = single("date", PrimitiveKind.INT32, bloomOver(h));
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("date").eq(date), blooms))
                .isInstanceOf(PruningDecision.Inconclusive.class);
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("date").eq(date.plusDays(1)), blooms))
                .isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void dateValueAgainstNonInt32ColumnIsNotApplied() {
        // DateVal against an INT64 column doesn't fit Parquet's plain-encoding rule (DATE is always INT32 in spec).
        FilterPipeline.BloomFilterLookup blooms =
                single("d", PrimitiveKind.INT64, bloomOver(SplitBlockBloomFilter.hashInt64(0L)));
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("d").eq(java.time.LocalDate.of(2020, 1, 1)), blooms))
                .isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void timestampValueAgainstInt64ColumnIsHashed() {
        java.time.LocalDateTime ts = java.time.LocalDateTime.of(2020, 1, 1, 0, 0);
        long epochMillis = ts.toEpochSecond(java.time.ZoneOffset.UTC) * 1000L;
        FilterPipeline.BloomFilterLookup blooms =
                single("ts", PrimitiveKind.INT64, bloomOver(SplitBlockBloomFilter.hashInt64(epochMillis)));
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("ts").eq(ts, true), blooms))
                .isInstanceOf(PruningDecision.Inconclusive.class);
        assertThat(BloomFilterEvaluator.evaluate(Pred.col("ts").eq(ts.plusDays(1), true), blooms))
                .isInstanceOf(PruningDecision.Eliminated.class);
    }

    // --- helpers ---

    private static FilterPipeline.BloomFilterLookup empty() {
        return _ -> Optional.empty();
    }

    private static FilterPipeline.BloomFilterLookup single(String name, PrimitiveKind kind, SplitBlockBloomFilter bf) {
        Map<ColumnPath, FilterPipeline.ColumnBloom> map = new HashMap<>();
        map.put(ColumnPath.of(name), new FilterPipeline.ColumnBloom(kind, bf));
        return path -> Optional.ofNullable(map.get(path));
    }

    private static FilterPipeline.BloomFilterLookup twoColumn(
            String n1,
            PrimitiveKind k1,
            SplitBlockBloomFilter b1,
            String n2,
            PrimitiveKind k2,
            SplitBlockBloomFilter b2) {
        Map<ColumnPath, FilterPipeline.ColumnBloom> map = new HashMap<>();
        map.put(ColumnPath.of(n1), new FilterPipeline.ColumnBloom(k1, b1));
        map.put(ColumnPath.of(n2), new FilterPipeline.ColumnBloom(k2, b2));
        return path -> Optional.ofNullable(map.get(path));
    }

    /**
     * Builds a tiny 1-block SBBF containing exactly the supplied hashes. Lets the test name what's in the bloom without
     * dragging the file format into play.
     */
    private static SplitBlockBloomFilter bloomOver(long... hashes) {
        ByteBuffer bitset = ByteBuffer.allocate(BYTES_PER_BLOCK).order(LITTLE_ENDIAN);
        for (long h : hashes) {
            referenceInsert(bitset, h);
        }
        return new SplitBlockBloomFilter(MemorySegment.ofBuffer(bitset));
    }

    private static void referenceInsert(ByteBuffer bitset, long hash) {
        int numBlocks = bitset.capacity() / BYTES_PER_BLOCK;
        long top = (hash >>> 32) & 0xffff_ffffL;
        int blockIndex = (int) ((top * numBlocks) >>> 32);
        int key = (int) hash;
        ByteBuffer le = bitset.duplicate().order(LITTLE_ENDIAN);
        int blockByteOffset = blockIndex * BYTES_PER_BLOCK;
        for (int i = 0; i < 8; i++) {
            int wordOffset = blockByteOffset + i * 4;
            int word = le.getInt(wordOffset);
            int probe = 1 << ((key * SALT[i]) >>> 27);
            le.putInt(wordOffset, word | probe);
        }
    }
}
