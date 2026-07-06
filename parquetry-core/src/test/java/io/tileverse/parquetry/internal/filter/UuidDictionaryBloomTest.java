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
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.internal.read.page.Dictionary;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.UuidConverter;

/**
 * Verifies that a {@code UuidVal} predicate prunes through the DICTIONARY and BLOOM_FILTER tiers, driving the same
 * package-private {@code evaluate(...)} entry points the sibling evaluator tests use. {@link #LOW} and {@link #HIGH}
 * differ in their high 64 bits ({@code 0x0000...} vs {@code 0xFFFF...}) where signed and unsigned ordering diverge,
 * which is exercised through the {@code lt}/range path in {@link #dictionaryRangeUsesUnsignedUuidOrdering()}.
 */
class UuidDictionaryBloomTest {

    private static final int BYTES_PER_BLOCK = 32;
    private static final int[] SALT = {
        0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31
    };

    private static final UUID LOW = new UUID(0x0000000000000000L, 1L);
    private static final UUID HIGH = new UUID(0xFFFFFFFFFFFFFFFFL, 0L); // unsigned-greater than LOW
    private static final UUID ABSENT = new UUID(0x1111111111111111L, 0x2222222222222222L);

    @Test
    void dictionaryEqPresentUuidIsInconclusive() {
        FilterPipeline.DictionaryLookup dicts = single("id", uuidDict(LOW, HIGH));
        Predicate p = col("id").eq(LOW);
        assertThat(DictionaryEvaluator.evaluate(p, dicts)).isInstanceOf(PruningDecision.Inconclusive.class);
    }

    @Test
    void dictionaryEqAbsentUuidIsEliminated() {
        FilterPipeline.DictionaryLookup dicts = single("id", uuidDict(LOW, HIGH));
        Predicate p = col("id").eq(ABSENT);
        assertThat(DictionaryEvaluator.evaluate(p, dicts)).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void dictionaryRangeUsesUnsignedUuidOrdering() {
        // Every dictionary entry is unsigned-greater than or equal to LOW; lt(LOW) therefore eliminates. A signed
        // comparison would treat HIGH (high bits 0xFFFF...) as negative and keep the row group, which this guards
        // against.
        FilterPipeline.DictionaryLookup dicts = single("id", uuidDict(LOW, HIGH));
        Predicate p = col("id").lt(LOW);
        assertThat(DictionaryEvaluator.evaluate(p, dicts)).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void bloomEqPresentUuidIsInconclusive() {
        FilterPipeline.BloomFilterLookup blooms =
                single("id", PrimitiveKind.FIXED_LEN_BYTE_ARRAY, bloomOver(hashOf(LOW)));
        Predicate p = col("id").eq(LOW);
        assertThat(BloomFilterEvaluator.evaluate(p, blooms)).isInstanceOf(PruningDecision.Inconclusive.class);
    }

    @Test
    void bloomEqAbsentUuidIsEliminated() {
        FilterPipeline.BloomFilterLookup blooms =
                single("id", PrimitiveKind.FIXED_LEN_BYTE_ARRAY, bloomOver(hashOf(LOW)));
        Predicate p = col("id").eq(ABSENT);
        assertThat(BloomFilterEvaluator.evaluate(p, blooms)).isInstanceOf(PruningDecision.Eliminated.class);
    }

    // --- dictionary helpers ---

    private static Dictionary.FixedLenBinaryDict uuidDict(UUID... uuids) {
        List<MemorySegment> entries = new ArrayList<>(uuids.length);
        for (UUID uuid : uuids) {
            entries.add(UuidConverter.toReadOnlySegment(uuid));
        }
        return new Dictionary.FixedLenBinaryDict(entries, 16);
    }

    private static FilterPipeline.DictionaryLookup single(String name, Dictionary<?> dict) {
        Map<ColumnPath, Dictionary<?>> map = new HashMap<>();
        map.put(ColumnPath.of(name), dict);
        return path -> Optional.ofNullable(map.get(path));
    }

    // --- bloom helpers ---

    /** The bloom hash the evaluator computes for a {@code UuidVal}: the SBBF hash of the UUID's 16 raw bytes. */
    private static long hashOf(UUID uuid) {
        byte[] bytes = UuidConverter.toReadOnlySegment(uuid).toArray(JAVA_BYTE);
        return SplitBlockBloomFilter.hashBytes(bytes);
    }

    private static FilterPipeline.BloomFilterLookup single(String name, PrimitiveKind kind, SplitBlockBloomFilter bf) {
        Map<ColumnPath, FilterPipeline.ColumnBloom> map = new HashMap<>();
        map.put(ColumnPath.of(name), new FilterPipeline.ColumnBloom(kind, bf));
        return path -> Optional.ofNullable(map.get(path));
    }

    /** Builds a tiny 1-block SBBF containing exactly the supplied hashes (copied from BloomFilterEvaluatorTest). */
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
