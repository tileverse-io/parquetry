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
package io.tileverse.parquetry.internal.filter;

import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.MatchAction;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Verifies how the pushdown tiers handle a {@link Predicate.Quantified} over a physical repeated leaf. The contract is
 * the same for every tier: an existential {@link MatchAction#ANY} may only eliminate the whole row group (no element
 * anywhere in the chunk matches implies no row matches), but must never report a positive match such as
 * {@code PassedAll}. A row with an empty or all-null list has zero elements and therefore fails {@code ANY} even when
 * every present element matches; such rows still need record-level confirmation. {@link MatchAction#ALL} and
 * {@link MatchAction#ONE} can never prune because chunk-level stats aggregate across rows.
 *
 * <p>The reader is flat-only and cannot write a real {@code LIST<STRUCT>} file. The tiers are driven through stubbed
 * lookups keyed by the physical leaf path the assembler would expose.
 */
class QuantifiedPushdownTest {

    private static final ColumnPath LEAF = ColumnPath.of("addresses", "list", "element", "locality");
    private static final ColumnPath INT_LEAF = ColumnPath.of("scores", "list", "element");
    private static final long ROW_COUNT = 1000;

    @Test
    void statsAnyDelegatesAndEliminatesWhenValueOutsideRange() {
        FilterPipeline.ColumnStatsLookup cols = statsLookup("alpha", "omega");
        Predicate quantified = quantified(MatchAction.ANY, "zzzz");
        PruningDecision decision = StatsEvaluator.evaluate(quantified, cols, ROW_COUNT);
        assertThat(decision).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void statsAnyDelegatesAndKeepsWhenValueWithinRange() {
        FilterPipeline.ColumnStatsLookup cols = statsLookup("alpha", "omega");
        Predicate quantified = quantified(MatchAction.ANY, "mango");
        PruningDecision decision = StatsEvaluator.evaluate(quantified, cols, ROW_COUNT);
        assertThat(decision).isNotInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void statsAllNeverPrunesEvenWhenValueOutsideRange() {
        FilterPipeline.ColumnStatsLookup cols = statsLookup("alpha", "omega");
        Predicate quantified = quantified(MatchAction.ALL, "zzzz");
        PruningDecision decision = StatsEvaluator.evaluate(quantified, cols, ROW_COUNT);
        assertThat(decision).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void statsOneNeverPrunesEvenWhenValueOutsideRange() {
        FilterPipeline.ColumnStatsLookup cols = statsLookup("alpha", "omega");
        Predicate quantified = quantified(MatchAction.ONE, "zzzz");
        PruningDecision decision = StatsEvaluator.evaluate(quantified, cols, ROW_COUNT);
        assertThat(decision).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void statsAnyOverSingleDistinctIntChunkDoesNotPassAll() {
        // A chunk whose only value equals the probed one would PassedAll for a scalar Eq, but a repeated leaf may hold
        // empty-list rows that fail ANY; the tier must degrade to NotApplied rather than mark the row group MATCHED.
        FilterPipeline.ColumnStatsLookup cols = intStatsLookup(7, 7);
        Predicate quantified = quantifiedInt(MatchAction.ANY, 7);
        PruningDecision decision = StatsEvaluator.evaluate(quantified, cols, ROW_COUNT);
        assertThat(decision).isInstanceOf(PruningDecision.NotApplied.class);
        assertThat(decision).isNotInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void statsAnyOverIntChunkEliminatesWhenValueOutOfRange() {
        FilterPipeline.ColumnStatsLookup cols = intStatsLookup(7, 7);
        Predicate quantified = quantifiedInt(MatchAction.ANY, 99);
        PruningDecision decision = StatsEvaluator.evaluate(quantified, cols, ROW_COUNT);
        assertThat(decision).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void columnIndexDoesNotPruneAnyQuantified() {
        FilterPipeline.ColumnPageStatsLookup cols = emptyPageStats();
        Predicate quantified = quantifiedInt(MatchAction.ANY, 7);
        PruningDecision decision = ColumnIndexEvaluator.evaluate(quantified, cols, ROW_COUNT);
        assertThat(decision).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void columnIndexDoesNotPruneAllQuantified() {
        FilterPipeline.ColumnPageStatsLookup cols = emptyPageStats();
        Predicate quantified = quantifiedInt(MatchAction.ALL, 7);
        PruningDecision decision = ColumnIndexEvaluator.evaluate(quantified, cols, ROW_COUNT);
        assertThat(decision).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void bloomAnyDelegatesAndEliminatesWhenValueAbsent() {
        FilterPipeline.BloomFilterLookup blooms = bloomLookup("Berlin");
        Predicate quantified = quantified(MatchAction.ANY, "Paris");
        PruningDecision decision = BloomFilterEvaluator.evaluate(quantified, blooms);
        assertThat(decision).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void bloomAnyDelegatesAndKeepsWhenValuePresent() {
        FilterPipeline.BloomFilterLookup blooms = bloomLookup("Berlin");
        Predicate quantified = quantified(MatchAction.ANY, "Berlin");
        PruningDecision decision = BloomFilterEvaluator.evaluate(quantified, blooms);
        assertThat(decision).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void bloomAllNeverPrunesEvenWhenValueAbsent() {
        FilterPipeline.BloomFilterLookup blooms = bloomLookup("Berlin");
        Predicate quantified = quantified(MatchAction.ALL, "Paris");
        PruningDecision decision = BloomFilterEvaluator.evaluate(quantified, blooms);
        assertThat(decision).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void bloomOneNeverPrunesEvenWhenValueAbsent() {
        FilterPipeline.BloomFilterLookup blooms = bloomLookup("Berlin");
        Predicate quantified = quantified(MatchAction.ONE, "Paris");
        PruningDecision decision = BloomFilterEvaluator.evaluate(quantified, blooms);
        assertThat(decision).isInstanceOf(PruningDecision.NotApplied.class);
    }

    private static Predicate quantified(MatchAction match, String value) {
        return new Predicate.Quantified(match, new Predicate.Eq(LEAF, new Value.StringVal(value)));
    }

    private static Predicate quantifiedInt(MatchAction match, int value) {
        return new Predicate.Quantified(match, new Predicate.Eq(INT_LEAF, new Value.IntVal(value)));
    }

    private static FilterPipeline.ColumnStatsLookup statsLookup(String min, String max) {
        Map<ColumnPath, FilterPipeline.ColumnStats> map = new HashMap<>();
        Statistics stats = Statistics.builder()
                .nullCount(OptionalLong.of(0L))
                .minValue(MemorySegment.ofArray(min.getBytes(StandardCharsets.UTF_8)))
                .maxValue(MemorySegment.ofArray(max.getBytes(StandardCharsets.UTF_8)))
                .build();
        map.put(LEAF, new FilterPipeline.ColumnStats(PrimitiveKind.BYTE_ARRAY, stats));
        return path -> Optional.ofNullable(map.get(path));
    }

    private static FilterPipeline.ColumnStatsLookup intStatsLookup(int min, int max) {
        Map<ColumnPath, FilterPipeline.ColumnStats> map = new HashMap<>();
        Statistics stats = Statistics.builder()
                .nullCount(OptionalLong.of(0L))
                .minValue(encodeInt(min))
                .maxValue(encodeInt(max))
                .build();
        map.put(INT_LEAF, new FilterPipeline.ColumnStats(PrimitiveKind.INT32, stats));
        return path -> Optional.ofNullable(map.get(path));
    }

    private static MemorySegment encodeInt(int value) {
        MemorySegment segment = MemorySegment.ofArray(new byte[4]);
        segment.set(INT32, 0, value);
        return segment.asReadOnly();
    }

    private static FilterPipeline.ColumnPageStatsLookup emptyPageStats() {
        return path -> Optional.empty();
    }

    private static FilterPipeline.BloomFilterLookup bloomLookup(String present) {
        long hash = SplitBlockBloomFilter.hashBytes(present.getBytes(StandardCharsets.UTF_8));
        Map<ColumnPath, FilterPipeline.ColumnBloom> map = new HashMap<>();
        map.put(LEAF, new FilterPipeline.ColumnBloom(PrimitiveKind.BYTE_ARRAY, bloomOver(hash)));
        return path -> Optional.ofNullable(map.get(path));
    }

    private static final int BYTES_PER_BLOCK = 32;
    private static final int[] SALT = {
        0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31
    };

    private static SplitBlockBloomFilter bloomOver(long... hashes) {
        ByteBuffer bitset = ByteBuffer.allocate(BYTES_PER_BLOCK).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        for (long hash : hashes) {
            referenceInsert(bitset, hash);
        }
        return new SplitBlockBloomFilter(MemorySegment.ofBuffer(bitset));
    }

    private static void referenceInsert(ByteBuffer bitset, long hash) {
        int numBlocks = bitset.capacity() / BYTES_PER_BLOCK;
        long top = (hash >>> 32) & 0xffff_ffffL;
        int blockIndex = (int) ((top * numBlocks) >>> 32);
        int key = (int) hash;
        ByteBuffer le = bitset.duplicate().order(java.nio.ByteOrder.LITTLE_ENDIAN);
        int blockByteOffset = blockIndex * BYTES_PER_BLOCK;
        for (int i = 0; i < 8; i++) {
            int wordOffset = blockByteOffset + i * 4;
            int word = le.getInt(wordOffset);
            int probe = 1 << ((key * SALT[i]) >>> 27);
            le.putInt(wordOffset, word | probe);
        }
    }
}
