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
package io.tileverse.parquetry.filter.bloom;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * Direct unit coverage for {@link BloomFilterReader}: both the single-I/O fast path (when bloom_filter_length is known)
 * and the two-step fallback (when it isn't), plus the rejection paths for malformed / unsupported sections. Synthesizes
 * the on-disk bytes itself so the tests don't depend on a particular parquet-java writer behavior.
 */
class BloomFilterReaderTest {

    private static final int BYTES_PER_BLOCK = 32;
    private static final int[] SALT = {
        0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31
    };

    @Test
    void readKnownLengthRoundTrip(@TempDir Path tmp) throws IOException {
        byte[] chunk = synthesizeBloomChunk(1, new long[] {SplitBlockBloomFilter.hashInt32(42)});
        Path file = writeBytes(tmp.resolve("bloom-known.bin"), chunk);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            SplitBlockBloomFilter bloom = BloomFilterReader.read(source, 0L, chunk.length);
            assertThat(bloom.mightContainInt(42)).isTrue();
            assertThat(bloom.mightContainInt(99)).isFalse();
        }
    }

    @Test
    void readWithoutLengthRoundTrip(@TempDir Path tmp) throws IOException {
        byte[] chunk = synthesizeBloomChunk(1, new long[] {SplitBlockBloomFilter.hashInt32(123)});
        // Append trailing bytes so the 128-byte header probe will overshoot the bitset; this is the realistic case
        // (the chunk is somewhere in the middle of a file, with other data after).
        byte[] padded = padWith(chunk, 200);
        Path file = writeBytes(tmp.resolve("bloom-unknown.bin"), padded);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            SplitBlockBloomFilter bloom = BloomFilterReader.readWithoutLength(source, 0L);
            assertThat(bloom.mightContainInt(123)).isTrue();
            assertThat(bloom.mightContainInt(7)).isFalse();
        }
    }

    @Test
    void readWithoutLengthSingleProbeBitsetFitsInProbe(@TempDir Path tmp) throws IOException {
        // 32-byte bitset + a sub-32-byte header fits entirely within the 128-byte probe, so no second fetch is needed.
        byte[] chunk = synthesizeBloomChunk(1, new long[] {SplitBlockBloomFilter.hashInt32(1)});
        // Tail with no padding: simulate "exactly bloom chunk at end of file"; readWithoutLength must still cope.
        Path file = writeBytes(tmp.resolve("bloom-eof.bin"), chunk);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            SplitBlockBloomFilter bloom = BloomFilterReader.readWithoutLength(source, 0L);
            assertThat(bloom.mightContainInt(1)).isTrue();
        }
    }

    @Test
    void readRejectsNonPositiveLength(@TempDir Path tmp) throws IOException {
        Path file = writeBytes(tmp.resolve("bloom-bad.bin"), new byte[16]);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            assertThatThrownBy(() -> BloomFilterReader.read(source, 0L, 0))
                    .isInstanceOf(ParquetFormatException.class)
                    .hasMessageContaining("Invalid bloom filter length");
            assertThatThrownBy(() -> BloomFilterReader.read(source, 0L, -10))
                    .isInstanceOf(ParquetFormatException.class);
        }
    }

    @Test
    void readRejectsTruncatedBitset(@TempDir Path tmp) throws IOException {
        // Header says numBytes=32 but the chunk is too short for that.
        byte[] chunk = synthesizeBloomChunk(1, new long[] {SplitBlockBloomFilter.hashInt32(1)});
        byte[] truncated = new byte[chunk.length - 8];
        System.arraycopy(chunk, 0, truncated, 0, truncated.length);
        Path file = writeBytes(tmp.resolve("bloom-truncated.bin"), truncated);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            assertThatThrownBy(() -> BloomFilterReader.read(source, 0L, truncated.length))
                    .isInstanceOf(ParquetFormatException.class)
                    .hasMessageContaining("truncated");
        }
    }

    @Test
    void decodeExposesBitsetSliceWithCorrectBounds() {
        byte[] chunk = synthesizeBloomChunk(2, new long[] {SplitBlockBloomFilter.hashInt64(123L)});
        SplitBlockBloomFilter bloom = BloomFilterReader.decode(ByteBuffer.wrap(chunk));
        assertThat(bloom.byteSize()).isEqualTo(2 * BYTES_PER_BLOCK);
        assertThat(bloom.mightContain(SplitBlockBloomFilter.hashInt64(123L))).isTrue();
    }

    // --- helpers ---

    /**
     * Synthesizes a complete bloom-filter chunk (header + bitset) in the canonical SPLIT_BLOCK / XXHASH / UNCOMPRESSED
     * shape. Writes the Thrift-compact header manually so we don't pull the deserializer's mirror into the test.
     */
    private static byte[] synthesizeBloomChunk(int numBlocks, long[] insertHashes) {
        int bitsetBytes = numBlocks * BYTES_PER_BLOCK;
        byte[] header = synthesizeHeader(bitsetBytes);
        byte[] bitset = new byte[bitsetBytes];
        ByteBuffer view = ByteBuffer.wrap(bitset).order(LITTLE_ENDIAN);
        for (long h : insertHashes) {
            referenceInsert(view, h, numBlocks);
        }
        byte[] out = new byte[header.length + bitsetBytes];
        System.arraycopy(header, 0, out, 0, header.length);
        System.arraycopy(bitset, 0, out, header.length, bitsetBytes);
        return out;
    }

    /**
     * Writes the Thrift compact header for {@code BloomFilterHeader(numBytes, SPLIT_BLOCK, XXHASH, UNCOMPRESSED)}. The
     * compact protocol delta-encodes field IDs; since we always emit consecutive fields (1, 2, 3, 4) each header byte
     * is {@code (1 << 4) | type}.
     */
    private static byte[] synthesizeHeader(int numBytes) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        // field 1 (delta=1, i32) numBytes
        writeFieldHeader(out, 1, 0x5);
        writeVarint(out, zigZag32(numBytes));
        // field 2 (delta=1, struct) algorithm
        writeFieldHeader(out, 1, 0xC);
        writeEmptyVariant(out);
        // field 3 (delta=1, struct) hash
        writeFieldHeader(out, 1, 0xC);
        writeEmptyVariant(out);
        // field 4 (delta=1, struct) compression
        writeFieldHeader(out, 1, 0xC);
        writeEmptyVariant(out);
        out.write(0); // STOP of outer struct
        return out.toByteArray();
    }

    /**
     * Single-case Thrift union body: a single field-header (delta=1, type=STRUCT) followed by the case's own STOP, then
     * the union's STOP.
     */
    private static void writeEmptyVariant(java.io.ByteArrayOutputStream out) {
        writeFieldHeader(out, 1, 0xC);
        out.write(0); // end of case struct
        out.write(0); // end of union struct
    }

    /** Short-form compact field header: {@code (delta << 4) | type}; delta must be 1..15. */
    private static void writeFieldHeader(java.io.ByteArrayOutputStream out, int delta, int type) {
        out.write((delta << 4) | (type & 0xf));
    }

    private static int zigZag32(int n) {
        return (n << 1) ^ (n >> 31);
    }

    private static void writeVarint(java.io.ByteArrayOutputStream out, int value) {
        while ((value & ~0x7f) != 0) {
            out.write((value & 0x7f) | 0x80);
            value >>>= 7;
        }
        out.write(value & 0x7f);
    }

    private static byte[] padWith(byte[] chunk, int padBytes) {
        byte[] out = new byte[chunk.length + padBytes];
        System.arraycopy(chunk, 0, out, 0, chunk.length);
        return out;
    }

    private static Path writeBytes(Path file, byte[] bytes) throws IOException {
        Files.write(file, bytes);
        return file;
    }

    private static void referenceInsert(ByteBuffer bitset, long hash, int numBlocks) {
        long top = (hash >>> 32) & 0xffff_ffffL;
        int blockIndex = (int) ((top * numBlocks) >>> 32);
        int key = (int) hash;
        int blockByteOffset = blockIndex * BYTES_PER_BLOCK;
        for (int i = 0; i < 8; i++) {
            int wordOffset = blockByteOffset + i * 4;
            int word = bitset.getInt(wordOffset);
            int probe = 1 << ((key * SALT[i]) >>> 27);
            bitset.putInt(wordOffset, word | probe);
        }
    }

    @SuppressWarnings("unused")
    private static Optional<OptionalLong> unused() {
        return Optional.empty();
    }
}
