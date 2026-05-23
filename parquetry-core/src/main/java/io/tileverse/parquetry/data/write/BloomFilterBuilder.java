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
package io.tileverse.parquetry.data.write;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;

import io.tileverse.parquetry.data.WriteOptions.BloomFilterConfig;
import io.tileverse.parquetry.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.format.BloomFilterHeader;
import io.tileverse.parquetry.format.ParquetFormat;

import lombok.NonNull;

/**
 * Write-side counterpart of {@link SplitBlockBloomFilter}. Sizes the SBBF bitset from
 * {@link BloomFilterConfig}({@code fpp}, {@code ndv}), accepts typed insertions per primitive kind, and writes the
 * Thrift-compact {@link BloomFilterHeader} followed by the raw bitset bytes to a caller-supplied {@link OutputStream}.
 *
 * <p>Hashing reuses the {@code hash...} helpers on {@link SplitBlockBloomFilter}, and bit insertion delegates to
 * {@link SplitBlockBloomFilter#insertHash(MemorySegment, long)}, so a value added through this builder will read back
 * as "might contain" on a {@link SplitBlockBloomFilter} wrapping the same bytes -- byte-for-byte the same xxHash64 /
 * SBBF block math the reader probes against.
 *
 * <p>Sizing follows the Parquet SBBF formula: {@code numBits = -8 * ndv / ln(1 - fpp^(1/8))}, then rounded up to a
 * multiple of {@value SplitBlockBloomFilter#BYTES_PER_BLOCK} bytes (one SBBF block). A floor of one block keeps tiny
 * filters valid.
 */
public final class BloomFilterBuilder {

    private static final int K = 8;

    private final byte[] bitset;
    private final MemorySegment bitsetSegment;

    public BloomFilterBuilder(@NonNull BloomFilterConfig cfg) {
        int numBytes = optimalNumBytes(cfg.fpp(), cfg.ndv());
        this.bitset = new byte[numBytes];
        this.bitsetSegment = MemorySegment.ofArray(bitset);
    }

    /** Hashes {@code value} as a plain-encoded INT32 and flips the corresponding SBBF bits. */
    public void add(int value) {
        SplitBlockBloomFilter.insertHash(bitsetSegment, SplitBlockBloomFilter.hashInt32(value));
    }

    /** Hashes {@code value} as a plain-encoded INT64 and flips the corresponding SBBF bits. */
    public void add(long value) {
        SplitBlockBloomFilter.insertHash(bitsetSegment, SplitBlockBloomFilter.hashInt64(value));
    }

    /** Hashes {@code value}'s raw IEEE bits as a plain-encoded FLOAT and flips the corresponding SBBF bits. */
    public void add(float value) {
        SplitBlockBloomFilter.insertHash(bitsetSegment, SplitBlockBloomFilter.hashFloat(value));
    }

    /** Hashes {@code value}'s raw IEEE bits as a plain-encoded DOUBLE and flips the corresponding SBBF bits. */
    public void add(double value) {
        SplitBlockBloomFilter.insertHash(bitsetSegment, SplitBlockBloomFilter.hashDouble(value));
    }

    /**
     * Hashes {@code value} as a plain-encoded BOOLEAN (one byte, 0 or 1) and flips the corresponding SBBF bits. Parquet
     * does not formally specify a hash for BOOLEAN in BloomFilter.md, but exposing it here keeps the typed surface
     * symmetric with the rest of the primitive kinds.
     */
    public void add(boolean value) {
        byte[] one = {(byte) (value ? 1 : 0)};
        SplitBlockBloomFilter.insertHash(bitsetSegment, SplitBlockBloomFilter.hashBytes(one));
    }

    /**
     * Hashes the raw bytes of {@code binary} as a plain-encoded BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY value and flips the
     * corresponding SBBF bits. Callers retain ownership of the segment; the builder copies whichever bytes it needs
     * before returning.
     */
    public void add(@NonNull MemorySegment binary) {
        byte[] copy = binary.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE);
        SplitBlockBloomFilter.insertHash(bitsetSegment, SplitBlockBloomFilter.hashBytes(copy));
    }

    /**
     * Writes the bloom filter header + bitset bytes to {@code out} starting at its current position, and returns the
     * placement metadata the column-chunk writer needs to record {@code ColumnMetaData.bloom_filter_offset} and
     * {@code bloom_filter_length}.
     *
     * <p>The {@code byteOffset} the caller passes is propagated back unchanged on the returned placement: this method
     * only writes; it does not know where the surrounding file cursor sits.
     */
    public BloomFilterPlacement writeTo(@NonNull OutputStream out, long byteOffset) throws IOException {
        BloomFilterHeader header = new BloomFilterHeader(
                bitset.length,
                BloomFilterHeader.Algorithm.SPLIT_BLOCK,
                BloomFilterHeader.HashStrategy.XXHASH,
                BloomFilterHeader.Compression.UNCOMPRESSED);
        CountingOutputStream counted = new CountingOutputStream(out);
        ParquetFormat.writeBloomFilterHeader(counted, header);
        counted.write(bitset);
        return new BloomFilterPlacement(byteOffset, counted.bytesWritten());
    }

    /** Bytes-written tally for the surrounding file cursor. Convenience overload that starts at offset 0. */
    public BloomFilterPlacement writeTo(@NonNull OutputStream out) throws IOException {
        return writeTo(out, 0L);
    }

    /** Returns the bitset's byte length; matches what the emitted {@link BloomFilterHeader#numBytes()} reports. */
    public int bitsetByteSize() {
        return bitset.length;
    }

    /**
     * Returns the SBBF block-count sizing per the Parquet spec. The formula is the SBBF-specific {@code numBits = -8 *
     * ndv / ln(1 - fpp^(1/8))} (k=8 probes per insertion), rounded up to a multiple of one block to satisfy
     * {@link SplitBlockBloomFilter}'s length constraint.
     */
    private static int optimalNumBytes(double fpp, long ndv) {
        double exponent = Math.log(fpp) / K;
        double denominator = Math.log(1.0 - Math.exp(exponent));
        double numBits = -K * (double) ndv / denominator;
        long numBytes = (long) Math.ceil(numBits / 8.0);
        long blockBytes = SplitBlockBloomFilter.BYTES_PER_BLOCK;
        long rounded = ((numBytes + blockBytes - 1) / blockBytes) * blockBytes;
        long floored = Math.max(rounded, blockBytes);
        if (floored > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Bloom filter sizing overflows int: " + floored + " bytes");
        }
        return (int) floored;
    }

    /** Counts how many bytes were written to the wrapped stream so {@link #writeTo} can report total length. */
    private static final class CountingOutputStream extends OutputStream {

        private final OutputStream delegate;
        private int bytesWritten;

        CountingOutputStream(OutputStream delegate) {
            this.delegate = delegate;
        }

        @Override
        public void write(int b) throws IOException {
            delegate.write(b);
            bytesWritten++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            delegate.write(b, off, len);
            bytesWritten += len;
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        int bytesWritten() {
            return bytesWritten;
        }
    }
}
