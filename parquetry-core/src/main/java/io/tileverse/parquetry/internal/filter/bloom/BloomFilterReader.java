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
package io.tileverse.parquetry.internal.filter.bloom;

import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

import io.tileverse.parquetry.format.BloomFilterHeader;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.format.UnsupportedFeatureException;
import io.tileverse.parquetry.format.codec.ParquetFormatDeserializer;
import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * Fetches a column chunk's bloom-filter bytes on demand from a {@link ByteRangeSource} and decodes the header + bitset
 * into a ready-to-probe {@link SplitBlockBloomFilter}.
 *
 * <p>Reads exactly the bytes the column metadata advertises ({@code bloom_filter_offset} +
 * {@code bloom_filter_length}); never speculatively fetches more. Callers that want predicate-driven laziness (skip
 * fetching for columns the predicate doesn't reference) decide when to invoke {@link #read} - this class doesn't cache.
 *
 * <p>Currently only the spec-defined combination (split-block algorithm + xxHash64 + uncompressed) is decoded; other
 * combinations would need codec wiring (none are defined by parquet-format at time of writing) and are reported as a
 * {@link ParquetFormatException}. Files that omit {@code bloom_filter_length} are not supported here - the predicate
 * evaluator degrades to {@code NotApplied} for those rather than guessing a read length.
 */
public final class BloomFilterReader {

    private BloomFilterReader() {}

    /**
     * Probe size used when {@link ColumnMetaData#bloomFilterLength bloom_filter_length} is absent (pre-2.10 writers).
     * The Thrift-compact-encoded header tops out at ~32 bytes in practice; 128 gives plenty of slack for forward-compat
     * fields without risking a read past end-of-file when the bloom section is near the footer.
     */
    private static final int HEADER_PROBE_BYTES = 128;

    /**
     * Reads the bloom-filter chunk at {@code offset} of total {@code length} bytes from {@code source}, decoding the
     * header and wrapping the trailing {@code header.numBytes()} bytes as a {@link SplitBlockBloomFilter}. This is the
     * single-I/O fast path for files that record {@code bloom_filter_length}.
     *
     * @param source the file source; caller retains ownership
     * @param offset {@code ColumnMetaData.bloom_filter_offset}
     * @param length {@code ColumnMetaData.bloom_filter_length} (header + bitset)
     * @return the loaded bloom filter, ready for {@code mightContain} probes
     * @throws ParquetFormatException if the section uses an unsupported algorithm / hash / compression, the header /
     *     bitset are truncated, or the bitset size is not a positive multiple of 32 bytes
     */
    public static SplitBlockBloomFilter read(ByteRangeSource source, long offset, int length) {
        if (length <= 0) {
            throw new MalformedFileException("Invalid bloom filter length: " + length);
        }
        byte[] bytes = new byte[length];
        source.readFully(offset, MemorySegment.ofArray(bytes));
        return decode(ByteBuffer.wrap(bytes));
    }

    /**
     * Reads the bloom-filter chunk at {@code offset} when {@code bloom_filter_length} is unknown (older Parquet writers
     * pre-spec-2.10 omit the field). Uses a two-step fetch: a small probe to decode the header, then a bounded read of
     * exactly the header-declared bitset size.
     */
    public static SplitBlockBloomFilter readWithoutLength(ByteRangeSource source, long offset) {
        byte[] probeBytes = new byte[HEADER_PROBE_BYTES];
        int probed = source.read(offset, MemorySegment.ofArray(probeBytes));
        if (probed <= 0) {
            throw new MalformedFileException("No bloom filter bytes at offset " + offset);
        }
        ByteBuffer probe = ByteBuffer.wrap(probeBytes, 0, probed).slice();
        ByteBuffer headerView = probe.duplicate();
        BloomFilterHeader header = readHeader(headerView);
        rejectUnsupported(header);
        int headerByteLength = headerView.position() - probe.position();
        int remainingHeader = probe.limit() - probe.position() - headerByteLength;
        if (remainingHeader >= header.numBytes()) {
            // Bitset fit entirely within the probe - no second read needed.
            ByteBuffer bitset = probe.duplicate();
            bitset.position(probe.position() + headerByteLength);
            bitset.limit(probe.position() + headerByteLength + header.numBytes());
            return new SplitBlockBloomFilter(asReadOnlySegment(bitset));
        }
        long bitsetStart = offset + headerByteLength;
        byte[] bitsetBytes = new byte[header.numBytes()];
        source.readFully(bitsetStart, MemorySegment.ofArray(bitsetBytes));
        return new SplitBlockBloomFilter(asReadOnlySegment(ByteBuffer.wrap(bitsetBytes)));
    }

    /**
     * Decodes the bloom-filter chunk from an already-loaded buffer. Useful for tests that synthesize the bytes; the
     * production path uses {@link #read} above.
     *
     * @param chunk a buffer positioned at the start of the bloom-filter section; the buffer's position advances past
     *     the header and bitset as a side effect
     */
    public static SplitBlockBloomFilter decode(ByteBuffer chunk) {
        ByteBuffer cursor = chunk.duplicate();
        BloomFilterHeader header = readHeader(cursor);
        rejectUnsupported(header);
        int headerByteLength = cursor.position() - chunk.position();
        int bitsetStart = chunk.position() + headerByteLength;
        if (chunk.limit() - bitsetStart < header.numBytes()) {
            throw new MalformedFileException("Bloom filter bitset truncated: header declared " + header.numBytes()
                    + " bytes but " + (chunk.limit() - bitsetStart) + " remain in the chunk");
        }
        ByteBuffer bitset = chunk.duplicate();
        bitset.position(bitsetStart);
        bitset.limit(bitsetStart + header.numBytes());
        return new SplitBlockBloomFilter(asReadOnlySegment(bitset));
    }

    /**
     * Wraps {@code bitset}'s active region (between its current position and limit) as a read-only
     * {@link MemorySegment} view, suitable for {@link SplitBlockBloomFilter}'s constructor.
     * {@link MemorySegment#ofBuffer} already respects the buffer's position / limit; callers don't need a fresh slice.
     */
    private static MemorySegment asReadOnlySegment(ByteBuffer bitset) {
        return MemorySegment.ofBuffer(bitset).asReadOnly();
    }

    private static BloomFilterHeader readHeader(ByteBuffer chunk) {
        InputStream stream = new BoundedByteBufferInputStream(chunk);
        return ParquetFormatDeserializer.readBloomFilterHeader(stream);
    }

    /**
     * The Parquet spec only defines one algorithm / hash / compression combination today. Anything else can't be probed
     * without new decoder code. Refusing it explicitly avoids silently treating an unknown combination as the supported
     * one.
     */
    private static void rejectUnsupported(BloomFilterHeader header) {
        if (header.algorithm() != BloomFilterHeader.Algorithm.SPLIT_BLOCK) {
            throw new UnsupportedFeatureException("Unsupported bloom-filter algorithm: " + header.algorithm());
        }
        if (header.hash() != BloomFilterHeader.HashStrategy.XXHASH) {
            throw new UnsupportedFeatureException("Unsupported bloom-filter hash strategy: " + header.hash());
        }
        if (header.compression() != BloomFilterHeader.Compression.UNCOMPRESSED) {
            throw new UnsupportedFeatureException("Unsupported bloom-filter compression: " + header.compression());
        }
    }

    /**
     * Minimal {@link InputStream} adapter that reads from a {@link ByteBuffer} and advances its position. We don't want
     * a fresh array copy just to read a Thrift header; this lets the deserializer pull bytes off the same buffer the
     * caller passed in, leaving the position at the first bitset byte.
     */
    private static final class BoundedByteBufferInputStream extends InputStream {

        private final ByteBuffer buffer;

        BoundedByteBufferInputStream(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public int read() {
            return buffer.hasRemaining() ? (buffer.get() & 0xff) : -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (!buffer.hasRemaining()) {
                return -1;
            }
            int toRead = Math.min(len, buffer.remaining());
            buffer.get(b, off, toRead);
            return toRead;
        }

        @Override
        public int available() {
            return buffer.remaining();
        }
    }
}
