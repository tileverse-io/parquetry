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
package io.tileverse.parquetry.internal.filter.bloom;

import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;

import java.lang.foreign.MemorySegment;

import io.airlift.compress.v3.xxhash.XxHash64Hasher;
import lombok.NonNull;

/**
 * Read-only Parquet split-block bloom filter (SBBF), implementing the {@code filter_check} side of the algorithm
 * described in {@code parquet-format/BloomFilter.md}.
 *
 * <p>An SBBF is a flat array of 32-byte blocks, each block holding eight 32-bit words. A query hash selects one block
 * from its top 32 bits and probes eight bits within that block via a fixed salt table. {@code mightContain} returns
 * {@code true} when every probed bit is set ("probably present") and {@code false} when any is clear ("definitely
 * absent"). Bloom filters never have false negatives - that's what makes them safe for predicate pruning.
 *
 * <h2>Hashing</h2>
 *
 * <p>The Parquet spec mandates xxHash64 (seed 0) over the value's plain encoding:
 *
 * <ul>
 *   <li>{@code INT32} / {@code INT64} - little-endian 4 / 8 bytes
 *   <li>{@code FLOAT} / {@code DOUBLE} - little-endian bit pattern (4 / 8 bytes)
 *   <li>{@code BYTE_ARRAY} / {@code FIXED_LEN_BYTE_ARRAY} - the raw value bytes
 * </ul>
 *
 * <p>Static {@code hash...} helpers compute these for the parquetry filter pipeline. {@code INT96} is not supported by
 * the Parquet bloom-filter spec and has no hash method here.
 *
 * <h2>Thread safety</h2>
 *
 * <p>Instances are immutable once constructed (no insert side). The probe loop in {@link #mightContain(long)} reads the
 * bitset through a {@link MemorySegment#asReadOnly() read-only} {@link MemorySegment}, so concurrent calls are safe and
 * the segment's contents cannot be mutated via this instance. Callers remain responsible for not mutating any heap or
 * native memory backing the segment after handing it over.
 */
public final class SplitBlockBloomFilter {

    /** Each block is 256 bits = 32 bytes = 8 words of 4 bytes. */
    public static final int BYTES_PER_BLOCK = 32;

    /** Each block has 8 set/checked bits, one per word. */
    private static final int BITS_PER_BLOCK_WORD = 8;

    /**
     * Eight odd 32-bit salts from {@code BloomFilter.md}. Multiplying the lower 32 bits of the value-hash by each salt
     * yields a per-word selector; the top 5 bits of that selector pick the bit position within the word.
     */
    private static final int[] SALT = {
        0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31
    };

    private final MemorySegment bitset;

    private final int numBlocks;

    /**
     * Wraps an already-loaded bitset.
     *
     * @param bitset the raw {@code numBytes}-byte bitset that follows the {@code BloomFilterHeader}; must have
     *     {@link MemorySegment#byteSize() byteSize} a positive multiple of {@value #BYTES_PER_BLOCK}. The segment is
     *     stored as a {@link MemorySegment#asReadOnly() read-only} view so this instance cannot mutate its contents;
     *     reads use a little-endian layout regardless of platform byte order.
     */
    public SplitBlockBloomFilter(@NonNull MemorySegment bitset) {
        long length = bitset.byteSize();
        if (length <= 0 || length % BYTES_PER_BLOCK != 0) {
            throw new IllegalArgumentException(
                    "Bloom filter bitset length must be a positive multiple of " + BYTES_PER_BLOCK + ", got " + length);
        }
        this.bitset = bitset.asReadOnly();
        this.numBlocks = (int) (length / BYTES_PER_BLOCK);
    }

    /** Bitset byte length; matches {@code BloomFilterHeader.numBytes()}. */
    public int byteSize() {
        return numBlocks * BYTES_PER_BLOCK;
    }

    /**
     * Returns {@code false} only when {@code hash} is definitely absent from the filter. A {@code true} return means
     * the value was either inserted or is a false-positive of the bloom filter.
     */
    public boolean mightContain(long hash) {
        int blockIndex = blockIndexFor(hash);
        int key = (int) hash;
        int wordBase = blockIndex * BITS_PER_BLOCK_WORD;
        // Position the bitset reads at the block's first word; each word is 4 bytes (little-endian).
        int blockByteOffset = wordBase * 4;
        for (int i = 0; i < BITS_PER_BLOCK_WORD; i++) {
            int wordOffset = blockByteOffset + i * 4;
            int word = bitset.get(INT32, wordOffset);
            int probe = 1 << probeBit(key, i);
            if ((word & probe) == 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns {@code mightContain(hashLittleEndianInt(value))}, the bloom probe for a plain-encoded {@code INT32}
     * column value.
     */
    public boolean mightContainInt(int value) {
        return mightContain(hashInt32(value));
    }

    /** Bloom probe for a plain-encoded {@code INT64} column value. */
    public boolean mightContainLong(long value) {
        return mightContain(hashInt64(value));
    }

    /** Bloom probe for a plain-encoded {@code FLOAT} column value (uses {@link Float#floatToRawIntBits}). */
    public boolean mightContainFloat(float value) {
        return mightContain(hashFloat(value));
    }

    /** Bloom probe for a plain-encoded {@code DOUBLE} column value (uses {@link Double#doubleToRawLongBits}). */
    public boolean mightContainDouble(double value) {
        return mightContain(hashDouble(value));
    }

    /** Bloom probe for the raw bytes of a {@code BYTE_ARRAY} or {@code FIXED_LEN_BYTE_ARRAY} column value. */
    public boolean mightContainBytes(byte[] bytes) {
        return mightContain(hashBytes(bytes));
    }

    /** Bloom probe for {@code bytes[offset .. offset+length)}. */
    public boolean mightContainBytes(byte[] bytes, int offset, int length) {
        return mightContain(hashBytes(bytes, offset, length));
    }

    /** xxHash64 of {@code value} encoded as 4 little-endian bytes; matches Parquet's plain-encoding hash for INT32. */
    public static long hashInt32(int value) {
        byte[] bytes = new byte[4];
        MemorySegment.ofArray(bytes).set(INT32, 0, value);
        return XxHash64Hasher.hash(bytes);
    }

    /** xxHash64 of {@code value} encoded as 8 little-endian bytes; matches Parquet's plain-encoding hash for INT64. */
    public static long hashInt64(long value) {
        byte[] bytes = new byte[8];
        MemorySegment.ofArray(bytes).set(INT64, 0, value);
        return XxHash64Hasher.hash(bytes);
    }

    /**
     * xxHash64 of the {@link Float#floatToRawIntBits raw IEEE bits} of {@code value} encoded as 4 little-endian bytes;
     * matches Parquet's plain-encoding hash for FLOAT.
     */
    public static long hashFloat(float value) {
        return hashInt32(Float.floatToRawIntBits(value));
    }

    /**
     * xxHash64 of the {@link Double#doubleToRawLongBits raw IEEE bits} of {@code value} encoded as 8 little-endian
     * bytes; matches Parquet's plain-encoding hash for DOUBLE.
     */
    public static long hashDouble(double value) {
        return hashInt64(Double.doubleToRawLongBits(value));
    }

    /** xxHash64 of {@code bytes}; matches Parquet's plain-encoding hash for BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY. */
    public static long hashBytes(byte[] bytes) {
        return XxHash64Hasher.hash(bytes);
    }

    /** xxHash64 of {@code bytes[offset .. offset+length)}. */
    public static long hashBytes(byte[] bytes, int offset, int length) {
        return XxHash64Hasher.hash(bytes, offset, length);
    }

    /** xxHash64 of the segment's bytes; matches {@link #hashBytes(byte[])} over the same bytes. */
    public static long hashBytes(@NonNull MemorySegment bytes) {
        return XxHash64Hasher.hash(bytes);
    }

    /**
     * Inserts {@code hash} into the writable bitset at {@code writableBitset}. The bitset's byte length must be a
     * positive multiple of {@link #BYTES_PER_BLOCK}; otherwise an {@link IllegalArgumentException} is raised. Uses the
     * exact block-and-salt math {@link #mightContain(long)} probes against, so a hash inserted here is guaranteed to
     * read back as {@code true} on a later {@link #mightContain(long)} call over the same bytes.
     *
     * <p>This is the write-side counterpart of {@link #mightContain(long)}. The class itself remains immutable -- this
     * method works against caller-owned bytes; readers wrap the same bytes in a fresh {@link SplitBlockBloomFilter} for
     * probing.
     */
    public static void insertHash(@NonNull MemorySegment writableBitset, long hash) {
        long length = writableBitset.byteSize();
        if (length <= 0 || length % BYTES_PER_BLOCK != 0) {
            throw new IllegalArgumentException(
                    "Bloom filter bitset length must be a positive multiple of " + BYTES_PER_BLOCK + ", got " + length);
        }
        int numBlocks = (int) (length / BYTES_PER_BLOCK);
        int blockIndex = blockIndexFor(hash, numBlocks);
        int key = (int) hash;
        int wordBase = blockIndex * BITS_PER_BLOCK_WORD;
        int blockByteOffset = wordBase * 4;
        for (int i = 0; i < BITS_PER_BLOCK_WORD; i++) {
            int wordOffset = blockByteOffset + i * 4;
            int word = writableBitset.get(INT32, wordOffset);
            int probe = 1 << probeBit(key, i);
            writableBitset.set(INT32, wordOffset, word | probe);
        }
    }

    /** Top 32 bits of the value hash select the block via {@code (top32 * numBlocks) >>> 32}. */
    private int blockIndexFor(long hash) {
        return blockIndexFor(hash, numBlocks);
    }

    private static int blockIndexFor(long hash, int numBlocks) {
        long top = (hash >>> 32) & 0xffff_ffffL;
        return (int) ((top * numBlocks) >>> 32);
    }

    /** Within a block, the i-th word's probed bit is {@code (key * SALT[i]) >>> 27} (0..31). */
    private static int probeBit(int key, int wordIndex) {
        return (key * SALT[wordIndex]) >>> 27;
    }
}
