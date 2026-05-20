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
package io.tileverse.parquetry.filter.bloom;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

/**
 * Cross-checks parquetry's {@link SplitBlockBloomFilter} against a hand-rolled reference of the parquet-format SBBF
 * pseudocode (see {@code parquet-format/BloomFilter.md}). The reference and parquetry implementations both probe the
 * same 8-block, 8-word-per-block, 8-bits-per-word structure with the same salt table, so a probe difference would mean
 * one of them is wrong. The reference is deliberately ugly (loops, no buffer tricks) so it's hard to copy a parquetry
 * bug into it accidentally.
 */
class SplitBlockBloomFilterTest {

    private static final int[] SALT = {
        0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31
    };
    private static final int BYTES_PER_BLOCK = 32;

    @Test
    void rejectsBitsetNotMultipleOf32Bytes() {
        ByteBuffer odd = ByteBuffer.allocate(31);
        MemorySegment segment = MemorySegment.ofBuffer(odd);
        assertThatThrownBy(() -> new SplitBlockBloomFilter(segment))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multiple of 32");
    }

    @Test
    void singleBlockProbesMatchReference() {
        // 1-block filter (32 bytes). Insert a handful of hashes via the reference encoder; check that
        // parquetry's mightContain agrees on both presence and absence.
        long[] inserted = {0x0L, 0x1L, 0xdeadbeefL, 0xfeedfaceL, 0x123456789abcdef0L};
        long[] notInserted = {0x2L, 0xbadc0ffee0ddf00dL, 0xfacefeedL, 0xcafebabeL};

        ByteBuffer bitset = newEmptyBitset(1);
        for (long h : inserted) {
            referenceInsert(bitset, h);
        }
        SplitBlockBloomFilter sbbf = new SplitBlockBloomFilter(MemorySegment.ofBuffer(bitset.duplicate()));

        for (long h : inserted) {
            assertThat(sbbf.mightContain(h))
                    .as("inserted hash 0x%x must be reported present", h)
                    .isTrue();
        }
        // A single-block bloom holding five inserts is dense enough that some absent probes will still trip on bits
        // that were set by the inserts (legitimate false positives). The contract we assert here is the bare minimum:
        // at least one absent hash has to come back false, otherwise the probe path is broken regardless of density.
        boolean anyAbsent = false;
        for (long h : notInserted) {
            if (!sbbf.mightContain(h)) {
                anyAbsent = true;
                break;
            }
        }
        assertThat(anyAbsent)
                .as("expected at least one not-inserted hash to be rejected")
                .isTrue();
    }

    @Test
    void multiBlockBloomEliminatesAbsent() {
        // 256-block filter (8 KB). With 100 inserts in this size, the false-positive rate is near zero, so we can
        // require ~all "not inserted" hashes to be rejected.
        ByteBuffer bitset = newEmptyBitset(256);
        for (int i = 0; i < 100; i++) {
            referenceInsert(bitset, mix(i + 1));
        }
        SplitBlockBloomFilter sbbf = new SplitBlockBloomFilter(MemorySegment.ofBuffer(bitset.duplicate()));

        for (int i = 0; i < 100; i++) {
            assertThat(sbbf.mightContain(mix(i + 1)))
                    .as("inserted hash i=%d must be reported present", i)
                    .isTrue();
        }
        int falsePositives = 0;
        for (int i = 1000; i < 1100; i++) {
            if (sbbf.mightContain(mix(i + 1))) {
                falsePositives++;
            }
        }
        // 100 probes against a sparsely populated 8KB SBBF: expect zero false positives at this density.
        assertThat(falsePositives)
                .as("100 absent hashes against a sparsely populated 8KB SBBF should not trip the bloom")
                .isZero();
    }

    @Test
    void plainEncodingHashesAreLittleEndian() {
        long expectedInt = SplitBlockBloomFilter.hashBytes(new byte[] {0x2a, 0x00, 0x00, 0x00});
        assertThat(SplitBlockBloomFilter.hashInt32(42)).isEqualTo(expectedInt);

        long expectedLong =
                SplitBlockBloomFilter.hashBytes(new byte[] {0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00});
        assertThat(SplitBlockBloomFilter.hashInt64(42L)).isEqualTo(expectedLong);
    }

    @Test
    void floatAndDoubleHashesMatchRawBitsLE() {
        float fv = 1.5f;
        double dv = 1.5;
        long fBits = SplitBlockBloomFilter.hashInt32(Float.floatToRawIntBits(fv));
        long dBits = SplitBlockBloomFilter.hashInt64(Double.doubleToRawLongBits(dv));
        assertThat(SplitBlockBloomFilter.hashFloat(fv)).isEqualTo(fBits);
        assertThat(SplitBlockBloomFilter.hashDouble(dv)).isEqualTo(dBits);
    }

    @Test
    void hashBytesOffsetMatchesFullSlice() {
        byte[] all = {0, 0, 'h', 'i', 0};
        long sliced = SplitBlockBloomFilter.hashBytes(all, 2, 2);
        long full = SplitBlockBloomFilter.hashBytes(new byte[] {'h', 'i'});
        assertThat(sliced).isEqualTo(full);
    }

    @Test
    void typedMightContainHelpersAgreeWithPlainHash() {
        // 1-block SBBF with one int / long / float / double / bytes insert, exercised via the typed convenience
        // wrappers. The wrappers must hash the value the same way the static hash* methods do.
        long hInt = SplitBlockBloomFilter.hashInt32(42);
        long hLong = SplitBlockBloomFilter.hashInt64(99L);
        long hFloat = SplitBlockBloomFilter.hashFloat(1.5f);
        long hDouble = SplitBlockBloomFilter.hashDouble(2.5);
        long hBytes = SplitBlockBloomFilter.hashBytes(new byte[] {1, 2, 3});

        SplitBlockBloomFilter intBloom = bloomWith(hInt);
        SplitBlockBloomFilter longBloom = bloomWith(hLong);
        SplitBlockBloomFilter floatBloom = bloomWith(hFloat);
        SplitBlockBloomFilter doubleBloom = bloomWith(hDouble);
        SplitBlockBloomFilter bytesBloom = bloomWith(hBytes);

        assertThat(intBloom.mightContainInt(42)).isTrue();
        assertThat(longBloom.mightContainLong(99L)).isTrue();
        assertThat(floatBloom.mightContainFloat(1.5f)).isTrue();
        assertThat(doubleBloom.mightContainDouble(2.5)).isTrue();
        assertThat(bytesBloom.mightContainBytes(new byte[] {1, 2, 3})).isTrue();
        assertThat(bytesBloom.mightContainBytes(new byte[] {0, 1, 2, 3, 0}, 1, 3))
                .isTrue();
    }

    @Test
    void byteSizeMatchesBitsetLength() {
        ByteBuffer bitset = newEmptyBitset(4);
        SplitBlockBloomFilter sbbf = new SplitBlockBloomFilter(MemorySegment.ofBuffer(bitset));
        assertThat(sbbf.byteSize()).isEqualTo(4 * BYTES_PER_BLOCK);
    }

    /** 1-block SBBF containing exactly the supplied hash, built via the reference inserter. */
    private static SplitBlockBloomFilter bloomWith(long hash) {
        ByteBuffer bitset = newEmptyBitset(1);
        referenceInsert(bitset, hash);
        return new SplitBlockBloomFilter(MemorySegment.ofBuffer(bitset));
    }

    /** Plain replica of parquet-format's SBBF {@code filter_insert} pseudocode; no buffer tricks. */
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

    private static ByteBuffer newEmptyBitset(int blocks) {
        return ByteBuffer.allocate(blocks * BYTES_PER_BLOCK).order(LITTLE_ENDIAN);
    }

    /**
     * Spreads sequential ints into a 64-bit avalanche so neighbouring values don't fall on the same block - mirrors how
     * a real value hash would look without dragging xxhash into the test.
     */
    private static long mix(int x) {
        long h = x * 0x9E3779B97F4A7C15L;
        h ^= h >>> 30;
        h *= 0xBF58476D1CE4E5B9L;
        h ^= h >>> 27;
        return h;
    }
}
