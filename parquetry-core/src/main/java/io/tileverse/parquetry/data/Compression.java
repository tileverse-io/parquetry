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
package io.tileverse.parquetry.data;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.brotli.dec.BrotliInputStream;

import io.tileverse.parquetry.format.CompressionCodec;

import io.airlift.compress.v3.hadoop.HadoopInputStream;
import io.airlift.compress.v3.lz4.Lz4Compressor;
import io.airlift.compress.v3.lz4.Lz4Decompressor;
import io.airlift.compress.v3.lz4.Lz4HadoopStreams;
import io.airlift.compress.v3.lzo.LzoCompressor;
import io.airlift.compress.v3.lzo.LzoDecompressor;
import io.airlift.compress.v3.snappy.SnappyCompressor;
import io.airlift.compress.v3.snappy.SnappyDecompressor;
import io.airlift.compress.v3.zstd.ZstdCompressor;
import io.airlift.compress.v3.zstd.ZstdDecompressor;

/**
 * Compression codec for Parquet page payloads.
 *
 * <p>Sealed ADT that doubles as the user-facing configuration on the write side and as the compress/decompress entry
 * point used internally by the page readers and writers. Construct variants via the static factories
 * ({@link #uncompressed()}, {@link #snappy()}, {@link #gzip()}, {@link #lz4Raw()}, {@link #zstd(int)},
 * {@link #brotli()}, {@link #lzo()}, {@link #lz4Hadoop()}).
 *
 * <p>{@link #wireCodec()} maps to the parquet-format wire enum ({@link CompressionCodec}) that lands in the footer's
 * {@link io.tileverse.parquetry.format.ColumnMetaData#codec() ColumnMetaData.codec}; {@link #forWireCodec} resolves a
 * wire enum value back to the default-parameterized case for the read path.
 *
 * <p>Three variants are decode-only: {@link Brotli} (the {@code org.brotli:dec} library ships only a decoder),
 * {@link Lz4Hadoop} (the deprecated LZ4 wire codec; new writers should emit {@link Lz4Raw} instead), and -- per the "do
 * not produce deprecated formats" rule -- nothing else. {@link Lzo} encodes and decodes both: the LZO wire codec is not
 * deprecated, just historically encumbered by C-library licensing.
 *
 * <p>Instances are stateless and safe to share across virtual threads; each compress/decompress call constructs its own
 * underlying compressor/decompressor so concurrent fan-out cannot race on internal scratch state.
 */
public sealed interface Compression
        permits Compression.Uncompressed,
                Compression.Snappy,
                Compression.Gzip,
                Compression.Lz4Raw,
                Compression.Zstd,
                Compression.Brotli,
                Compression.Lzo,
                Compression.Lz4Hadoop {

    /** No compression; page bytes are emitted verbatim. */
    static Compression uncompressed() {
        return Uncompressed.INSTANCE;
    }

    /** Snappy framing as used by other Parquet writers. */
    static Compression snappy() {
        return Snappy.INSTANCE;
    }

    /** GZIP / deflate; broadest reader compatibility, slowest of the modern codecs. */
    static Compression gzip() {
        return Gzip.INSTANCE;
    }

    /** LZ4 raw block format (the modern, non-deprecated LZ4 codec in parquet-format). */
    static Compression lz4Raw() {
        return Lz4Raw.INSTANCE;
    }

    /**
     * ZSTD at the supplied compression level.
     *
     * @param level zstd compression level; must be in {@code [1, 22]} (3 is the project default)
     */
    static Compression zstd(int level) {
        return new Zstd(level);
    }

    /** Brotli; supported for symmetry with the read side. Compression is not supported on this codec. */
    static Compression brotli() {
        return Brotli.INSTANCE;
    }

    /** LZO; rare in modern pipelines but not deprecated by the parquet-format spec. */
    static Compression lzo() {
        return Lzo.INSTANCE;
    }

    /**
     * LZ4 with Hadoop framing; this is the deprecated {@code LZ4} wire codec (use {@link #lz4Raw()} for new writes).
     * Decompression-only: the writer refuses to produce deprecated codecs.
     */
    static Compression lz4Hadoop() {
        return Lz4Hadoop.INSTANCE;
    }

    /** Wire-enum value for footer encoding; what lands in {@code ColumnMetaData.codec}. */
    CompressionCodec wireCodec();

    /**
     * Decompresses the contents of {@code src} into {@code output}. The {@code output} segment must be at least as
     * large as the expected uncompressed payload. Returns the number of bytes written.
     */
    int decompress(MemorySegment src, MemorySegment output) throws IOException;

    /**
     * Compresses {@code src} into {@code output}. Returns the number of compressed bytes written.
     *
     * <p>Implementations must respect {@code output.byteSize()} as a hard upper bound; if compressed output would
     * exceed it, throw {@link IOException} -- callers size the output via {@link #maxCompressedLength(long)}.
     *
     * @throws UnsupportedOperationException if this codec only supports decompression (currently only Brotli)
     */
    int compress(MemorySegment src, MemorySegment output) throws IOException;

    /** Maximum bytes the compressor might write for an input of length {@code uncompressedLength}. */
    long maxCompressedLength(long uncompressedLength);

    /**
     * Resolves a wire enum value to the default-parameterized case. Used on the read path where the wire form does not
     * carry codec parameters; ZSTD-level information is not present in the wire enum so this method returns
     * {@link #zstd(int) zstd(3)}.
     */
    static Compression forWireCodec(CompressionCodec wire) {
        return switch (wire) {
            case UNCOMPRESSED -> uncompressed();
            case SNAPPY -> snappy();
            case GZIP -> gzip();
            case LZ4_RAW -> lz4Raw();
            case ZSTD -> zstd(Zstd.DEFAULT_LEVEL);
            case BROTLI -> brotli();
            case LZO -> lzo();
            case LZ4 -> lz4Hadoop();
        };
    }

    /** No-op codec; compress and decompress copy bytes verbatim. */
    record Uncompressed() implements Compression {

        static final Uncompressed INSTANCE = new Uncompressed();

        @Override
        public CompressionCodec wireCodec() {
            return CompressionCodec.UNCOMPRESSED;
        }

        @Override
        public int decompress(MemorySegment src, MemorySegment output) {
            long size = src.byteSize();
            MemorySegment.copy(src, 0L, output, 0L, size);
            return (int) size;
        }

        @Override
        public int compress(MemorySegment src, MemorySegment output) throws IOException {
            long size = src.byteSize();
            if (size > output.byteSize()) {
                throw new IOException(
                        "UNCOMPRESSED output buffer too small: need " + size + " bytes, have " + output.byteSize());
            }
            MemorySegment.copy(src, 0L, output, 0L, size);
            return (int) size;
        }

        @Override
        public long maxCompressedLength(long uncompressedLength) {
            return uncompressedLength;
        }
    }

    /** Snappy codec; uses aircompressor's snappy compressor/decompressor. */
    record Snappy() implements Compression {

        static final Snappy INSTANCE = new Snappy();

        @Override
        public CompressionCodec wireCodec() {
            return CompressionCodec.SNAPPY;
        }

        @Override
        public int decompress(MemorySegment src, MemorySegment output) {
            return SnappyDecompressor.create().decompress(src, output);
        }

        @Override
        public int compress(MemorySegment src, MemorySegment output) throws IOException {
            SnappyCompressor compressor = SnappyCompressor.create();
            try {
                return compressor.compress(src, output);
            } catch (IllegalArgumentException overflow) {
                throw new IOException(
                        "SNAPPY output buffer too small for input of " + src.byteSize() + " bytes", overflow);
            }
        }

        @Override
        public long maxCompressedLength(long uncompressedLength) {
            return SnappyCompressor.create().maxCompressedLength(CompressionSupport.intExact(uncompressedLength));
        }
    }

    /**
     * GZIP codec. The JDK's {@link GZIPInputStream}/{@link GZIPOutputStream} are stream/byte-array based, so the
     * boundary with the codec's {@link MemorySegment} API is bridged via {@code byte[]} copies. Not on the hot path.
     *
     * <p>Aircompressor v3 ships only a hadoop-stream-based gzip codec (no standalone {@code Compressor}), so we drive
     * {@link GZIPOutputStream} directly. {@link #maxCompressedLength(long)} mirrors the conservative bound used by
     * {@code java.util.zip.Deflater}: the source length, rounded up to a 16 KiB boundary, plus a 64-byte header/trailer
     * allowance, with a 32-byte floor for empty/tiny payloads.
     */
    record Gzip() implements Compression {

        static final Gzip INSTANCE = new Gzip();

        private static final int HEADER_TRAILER_ALLOWANCE = 64;
        private static final int BLOCK_OVERHEAD_GRANULARITY = 16 * 1024;
        private static final long MIN_OUTPUT_SIZE = 32L;

        @Override
        public CompressionCodec wireCodec() {
            return CompressionCodec.GZIP;
        }

        @Override
        public int decompress(MemorySegment src, MemorySegment output) throws IOException {
            byte[] in = src.toArray(JAVA_BYTE);
            try (GZIPInputStream gz = new GZIPInputStream(new ByteArrayInputStream(in))) {
                return CompressionSupport.streamToSegment(gz, output);
            }
        }

        @Override
        public int compress(MemorySegment src, MemorySegment output) throws IOException {
            CompressionSupport.BoundedSegmentOutputStream sink =
                    new CompressionSupport.BoundedSegmentOutputStream(output);
            try (GZIPOutputStream gz = new GZIPOutputStream(sink)) {
                CompressionSupport.writeFromSegment(src, gz);
            }
            return sink.written();
        }

        @Override
        public long maxCompressedLength(long uncompressedLength) {
            // Java's Deflater accepts int-sized inputs only; capping here avoids overflow in the rounding arithmetic
            // below when uncompressedLength is near Long.MAX_VALUE.
            if (uncompressedLength < 0 || uncompressedLength > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("uncompressedLength out of range: " + uncompressedLength);
            }
            long blocks = (uncompressedLength + BLOCK_OVERHEAD_GRANULARITY - 1) / BLOCK_OVERHEAD_GRANULARITY;
            long overheadFromBlocks = blocks * 5L;
            long total = uncompressedLength + overheadFromBlocks + HEADER_TRAILER_ALLOWANCE;
            return Math.max(total, MIN_OUTPUT_SIZE);
        }
    }

    /**
     * LZ4_RAW codec. Uses aircompressor's {@link Lz4Compressor} / {@link Lz4Decompressor}, which handle the raw LZ4
     * block format (no frame header) that Parquet's LZ4_RAW encoding uses.
     */
    record Lz4Raw() implements Compression {

        static final Lz4Raw INSTANCE = new Lz4Raw();

        @Override
        public CompressionCodec wireCodec() {
            return CompressionCodec.LZ4_RAW;
        }

        @Override
        public int decompress(MemorySegment src, MemorySegment output) {
            return Lz4Decompressor.create().decompress(src, output);
        }

        @Override
        public int compress(MemorySegment src, MemorySegment output) throws IOException {
            Lz4Compressor compressor = Lz4Compressor.create();
            try {
                return compressor.compress(src, output);
            } catch (IllegalArgumentException overflow) {
                throw new IOException(
                        "LZ4_RAW output buffer too small for input of " + src.byteSize() + " bytes", overflow);
            }
        }

        @Override
        public long maxCompressedLength(long uncompressedLength) {
            return Lz4Compressor.create().maxCompressedLength(CompressionSupport.intExact(uncompressedLength));
        }
    }

    /**
     * ZSTD codec. The compression level (range {@code [1, 22]}, default {@value #DEFAULT_LEVEL}) is configured at
     * construction time.
     */
    record Zstd(int level) implements Compression {

        /** ZSTD default compression level, matching the underlying aircompressor default. */
        public static final int DEFAULT_LEVEL = 3;

        private static final int MIN_LEVEL = 1;
        private static final int MAX_LEVEL = 22;

        public Zstd {
            if (level < MIN_LEVEL || level > MAX_LEVEL) {
                throw new IllegalArgumentException(
                        "ZSTD compression level must be in [" + MIN_LEVEL + ", " + MAX_LEVEL + "], got " + level);
            }
        }

        @Override
        public CompressionCodec wireCodec() {
            return CompressionCodec.ZSTD;
        }

        @Override
        public int decompress(MemorySegment src, MemorySegment output) {
            return ZstdDecompressor.create().decompress(src, output);
        }

        @Override
        public int compress(MemorySegment src, MemorySegment output) throws IOException {
            ZstdCompressor compressor = compressorAtBestLevel();
            try {
                return compressor.compress(src, output);
            } catch (IllegalArgumentException overflow) {
                throw new IOException(
                        "ZSTD output buffer too small for input of " + src.byteSize() + " bytes", overflow);
            }
        }

        @Override
        public long maxCompressedLength(long uncompressedLength) {
            return compressorAtBestLevel().maxCompressedLength(CompressionSupport.intExact(uncompressedLength));
        }

        /**
         * A compressor at the configured {@link #level}, degrading to the backend default when only the non-native zstd
         * compressor is available (platforms without aircompressor's native zstd, such as Windows). That compressor
         * accepts only its default level and rejects any other. The level changes the compression ratio, not the wire
         * format; a file written at the default level still reads back identically, on every platform.
         *
         * <p>The level is already validated to {@code [1, 22]} by the constructor; a rejection here can only mean the
         * non-native backend declining a non-default level.
         */
        private ZstdCompressor compressorAtBestLevel() {
            try {
                return ZstdCompressor.create(level);
            } catch (IllegalArgumentException _) {
                return ZstdCompressor.create();
            }
        }
    }

    /**
     * Brotli codec. Decode-only: the {@code org.brotli:dec} library on the classpath is decoder-only and aircompressor
     * v3 does not ship a Brotli compressor, so {@link #compress(MemorySegment, MemorySegment)} and
     * {@link #maxCompressedLength(long)} throw {@link UnsupportedOperationException}. Writers must select a different
     * codec when configuring BROTLI output.
     */
    record Brotli() implements Compression {

        static final Brotli INSTANCE = new Brotli();

        private static final String COMPRESS_UNSUPPORTED_MESSAGE =
                "Brotli compression is not supported: the org.brotli:dec dependency is decoder-only "
                        + "and aircompressor v3 does not provide a Brotli compressor";

        @Override
        public CompressionCodec wireCodec() {
            return CompressionCodec.BROTLI;
        }

        @Override
        public int decompress(MemorySegment src, MemorySegment output) throws IOException {
            byte[] in = src.toArray(JAVA_BYTE);
            try (BrotliInputStream br = new BrotliInputStream(new ByteArrayInputStream(in))) {
                return CompressionSupport.streamToSegment(br, output);
            }
        }

        @Override
        public int compress(MemorySegment src, MemorySegment output) {
            throw new UnsupportedOperationException(COMPRESS_UNSUPPORTED_MESSAGE);
        }

        @Override
        public long maxCompressedLength(long uncompressedLength) {
            throw new UnsupportedOperationException(COMPRESS_UNSUPPORTED_MESSAGE);
        }
    }

    /**
     * LZO codec. Uses aircompressor's {@link LzoCompressor} / {@link LzoDecompressor} on raw LZO block data (no Hadoop
     * stream framing).
     */
    record Lzo() implements Compression {

        static final Lzo INSTANCE = new Lzo();

        @Override
        public CompressionCodec wireCodec() {
            return CompressionCodec.LZO;
        }

        @Override
        public int decompress(MemorySegment src, MemorySegment output) {
            return new LzoDecompressor().decompress(src, output);
        }

        @Override
        public int compress(MemorySegment src, MemorySegment output) throws IOException {
            LzoCompressor compressor = new LzoCompressor();
            try {
                return compressor.compress(src, output);
            } catch (IllegalArgumentException overflow) {
                throw new IOException(
                        "LZO output buffer too small for input of " + src.byteSize() + " bytes", overflow);
            }
        }

        @Override
        public long maxCompressedLength(long uncompressedLength) {
            return new LzoCompressor().maxCompressedLength(CompressionSupport.intExact(uncompressedLength));
        }
    }

    /**
     * Legacy LZ4 codec (Hadoop framing). The parquet-format spec deprecates this in favor of {@link Lz4Raw}, so the
     * writer side throws {@link UnsupportedOperationException} -- the reader still accepts existing files via
     * aircompressor's {@link Lz4HadoopStreams}.
     */
    record Lz4Hadoop() implements Compression {

        static final Lz4Hadoop INSTANCE = new Lz4Hadoop();

        private static final String COMPRESS_UNSUPPORTED_MESSAGE =
                "Legacy LZ4 (Hadoop framing) compression is not supported because the parquet-format spec "
                        + "deprecates this codec; use lz4Raw() for new writes";

        @Override
        public CompressionCodec wireCodec() {
            return CompressionCodec.LZ4;
        }

        @Override
        public int decompress(MemorySegment src, MemorySegment output) throws IOException {
            byte[] in = src.toArray(JAVA_BYTE);
            InputStream wrapped = new ByteArrayInputStream(in);
            try (HadoopInputStream hadoop = new Lz4HadoopStreams().createInputStream(wrapped)) {
                return CompressionSupport.streamToSegment(hadoop, output);
            }
        }

        @Override
        public int compress(MemorySegment src, MemorySegment output) {
            throw new UnsupportedOperationException(COMPRESS_UNSUPPORTED_MESSAGE);
        }

        @Override
        public long maxCompressedLength(long uncompressedLength) {
            throw new UnsupportedOperationException(COMPRESS_UNSUPPORTED_MESSAGE);
        }
    }
}
