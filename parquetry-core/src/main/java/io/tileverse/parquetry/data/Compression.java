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
package io.tileverse.parquetry.data;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

import io.tileverse.parquetry.compression.Codec;
import io.tileverse.parquetry.format.CompressionCodec;

/**
 * Compression codec for Parquet page payloads.
 *
 * <p>Sealed ADT that doubles as the user-facing configuration on the write side and as the compress/decompress entry
 * point used internally by the page readers and writers. Construct cases via the static factories
 * ({@link #uncompressed()}, {@link #snappy()}, {@link #gzip()}, {@link #lz4Raw()}, {@link #zstd(int)},
 * {@link #brotli()}, {@link #lzo()}, {@link #lz4Hadoop()}).
 *
 * <p>The byte-level work is delegated to the {@link Codec} engines in {@code parquetry-compression}; this type owns the
 * Parquet-side concerns: {@link #wireCodec()} maps to the parquet-format wire enum ({@link CompressionCodec}) that
 * lands in the footer's {@link io.tileverse.parquetry.format.ColumnMetaData#codec() ColumnMetaData.codec};
 * {@link #forWireCodec} resolves a wire enum value back to the default-parameterized case for the read path.
 *
 * <p>Three cases are decode-only: {@link Brotli} (the {@code org.brotli:dec} library ships only a decoder),
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
     * include codec parameters; ZSTD-level information is not present in the wire enum, thus this method returns
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

        private static final Codec ENGINE = Codec.uncompressed();

        @Override
        public CompressionCodec wireCodec() {
            return CompressionCodec.UNCOMPRESSED;
        }

        @Override
        public int decompress(MemorySegment src, MemorySegment output) throws IOException {
            return ENGINE.decompress(src, output);
        }

        @Override
        public int compress(MemorySegment src, MemorySegment output) throws IOException {
            return ENGINE.compress(src, output);
        }

        @Override
        public long maxCompressedLength(long uncompressedLength) {
            return ENGINE.maxCompressedLength(uncompressedLength);
        }
    }

    /** Snappy codec; raw snappy block format, as other Parquet writers emit. */
    record Snappy() implements Compression {

        static final Snappy INSTANCE = new Snappy();

        private static final Codec ENGINE = Codec.snappy();

        @Override
        public CompressionCodec wireCodec() {
            return CompressionCodec.SNAPPY;
        }

        @Override
        public int decompress(MemorySegment src, MemorySegment output) throws IOException {
            return ENGINE.decompress(src, output);
        }

        @Override
        public int compress(MemorySegment src, MemorySegment output) throws IOException {
            return ENGINE.compress(src, output);
        }

        @Override
        public long maxCompressedLength(long uncompressedLength) {
            return ENGINE.maxCompressedLength(uncompressedLength);
        }
    }

    /** GZIP codec. */
    record Gzip() implements Compression {

        static final Gzip INSTANCE = new Gzip();

        private static final Codec ENGINE = Codec.gzip();

        @Override
        public CompressionCodec wireCodec() {
            return CompressionCodec.GZIP;
        }

        @Override
        public int decompress(MemorySegment src, MemorySegment output) throws IOException {
            return ENGINE.decompress(src, output);
        }

        @Override
        public int compress(MemorySegment src, MemorySegment output) throws IOException {
            return ENGINE.compress(src, output);
        }

        @Override
        public long maxCompressedLength(long uncompressedLength) {
            return ENGINE.maxCompressedLength(uncompressedLength);
        }
    }

    /** LZ4_RAW codec: the raw LZ4 block format (no frame header) that Parquet's LZ4_RAW encoding uses. */
    record Lz4Raw() implements Compression {

        static final Lz4Raw INSTANCE = new Lz4Raw();

        private static final Codec ENGINE = Codec.lz4Raw();

        @Override
        public CompressionCodec wireCodec() {
            return CompressionCodec.LZ4_RAW;
        }

        @Override
        public int decompress(MemorySegment src, MemorySegment output) throws IOException {
            return ENGINE.decompress(src, output);
        }

        @Override
        public int compress(MemorySegment src, MemorySegment output) throws IOException {
            return ENGINE.compress(src, output);
        }

        @Override
        public long maxCompressedLength(long uncompressedLength) {
            return ENGINE.maxCompressedLength(uncompressedLength);
        }
    }

    /**
     * ZSTD codec. The compression level (range {@code [1, 22]}, default {@value #DEFAULT_LEVEL}) is configured at
     * construction time. The level changes the compression ratio, not the wire format; on platforms where only the
     * non-native zstd backend is available the engine degrades to the backend's default level.
     */
    record Zstd(int level) implements Compression {

        /** ZSTD default compression level, matching the underlying aircompressor default. */
        public static final int DEFAULT_LEVEL = Codec.Zstd.DEFAULT_LEVEL;

        public Zstd {
            // Delegates range validation (and its error message) to the engine constructor.
            Codec.zstd(level);
        }

        @Override
        public CompressionCodec wireCodec() {
            return CompressionCodec.ZSTD;
        }

        @Override
        public int decompress(MemorySegment src, MemorySegment output) throws IOException {
            return Codec.zstd(level).decompress(src, output);
        }

        @Override
        public int compress(MemorySegment src, MemorySegment output) throws IOException {
            return Codec.zstd(level).compress(src, output);
        }

        @Override
        public long maxCompressedLength(long uncompressedLength) {
            return Codec.zstd(level).maxCompressedLength(uncompressedLength);
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

        private static final Codec ENGINE = Codec.brotli();

        @Override
        public CompressionCodec wireCodec() {
            return CompressionCodec.BROTLI;
        }

        @Override
        public int decompress(MemorySegment src, MemorySegment output) throws IOException {
            return ENGINE.decompress(src, output);
        }

        @Override
        public int compress(MemorySegment src, MemorySegment output) throws IOException {
            return ENGINE.compress(src, output);
        }

        @Override
        public long maxCompressedLength(long uncompressedLength) {
            return ENGINE.maxCompressedLength(uncompressedLength);
        }
    }

    /** LZO codec; raw LZO block data (no Hadoop stream framing). */
    record Lzo() implements Compression {

        static final Lzo INSTANCE = new Lzo();

        private static final Codec ENGINE = Codec.lzo();

        @Override
        public CompressionCodec wireCodec() {
            return CompressionCodec.LZO;
        }

        @Override
        public int decompress(MemorySegment src, MemorySegment output) throws IOException {
            return ENGINE.decompress(src, output);
        }

        @Override
        public int compress(MemorySegment src, MemorySegment output) throws IOException {
            return ENGINE.compress(src, output);
        }

        @Override
        public long maxCompressedLength(long uncompressedLength) {
            return ENGINE.maxCompressedLength(uncompressedLength);
        }
    }

    /**
     * Legacy LZ4 codec. The parquet-format spec deprecates this in favor of {@link Lz4Raw}; the writer side throws
     * {@link UnsupportedOperationException} while the reader still accepts existing files.
     *
     * <p>The deprecated {@code LZ4} wire code is ambiguous: parquet-mr writes Hadoop-framed blocks, while parquet-cpp
     * writes raw LZ4 blocks with no frame. This reader tries the Hadoop frame first and falls back to raw decoding when
     * the frame does not yield the expected number of bytes, matching how arrow and parquet-cpp read the codec.
     */
    record Lz4Hadoop() implements Compression {

        static final Lz4Hadoop INSTANCE = new Lz4Hadoop();

        private static final Codec FRAMED = Codec.lz4Hadoop();
        private static final Codec RAW = Codec.lz4Raw();

        private static final String COMPRESS_UNSUPPORTED_MESSAGE =
                "Legacy LZ4 (Hadoop framing) compression is not supported because the parquet-format spec "
                        + "deprecates this codec; use lz4Raw() for new writes";

        @Override
        public CompressionCodec wireCodec() {
            return CompressionCodec.LZ4;
        }

        @Override
        public int decompress(MemorySegment src, MemorySegment output) throws IOException {
            try {
                int written = FRAMED.decompress(src, output);
                if (written == output.byteSize()) {
                    return written;
                }
            } catch (IOException _) {
                // Not a Hadoop-framed stream; fall through to raw LZ4 decoding.
            }
            return RAW.decompress(src, output);
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
