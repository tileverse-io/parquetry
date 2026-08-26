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
package io.tileverse.parquetry.compression;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.Inflater;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.brotli.dec.BrotliInputStream;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;

import io.airlift.compress.v3.MalformedInputException;
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
import io.airlift.compress.v3.zstd.ZstdInputStream;

/**
 * Compression codec engine over {@link MemorySegment} payloads.
 *
 * <p>Each case wraps one compression wire format and knows nothing about the container that uses it. The Parquet module
 * maps its footer codec enum onto these engines and keeps its own codec policy (which codecs a writer may produce); the
 * Avro module maps OCF codec names and the Avro-specific block framing onto the same engines.
 *
 * <p>Decompression comes in two shapes. {@link #decompress(MemorySegment, MemorySegment)} writes into a caller-sized
 * output for containers that record the uncompressed size next to the payload, as Parquet page headers do.
 * {@link #decompress(MemorySegment)} discovers the size from the compressed stream itself and returns a freshly
 * allocated read-only segment, for containers that do not record it, as Avro OCF blocks do. Formats that neither embed
 * the size nor frame the stream (raw LZ4 blocks, raw LZO blocks) cannot support the size-discovering shape and throw
 * {@link UnsupportedOperationException} from it.
 *
 * <p>Construct cases via the static factories ({@link #uncompressed()}, {@link #snappy()}, {@link #gzip()},
 * {@link #deflate()}, {@link #lz4Raw()}, {@link #lz4Hadoop()}, {@link #zstd(int)}, {@link #brotli()}, {@link #lzo()},
 * {@link #bzip2()}, {@link #xz()}).
 *
 * <p>Instances are stateless and safe to share across virtual threads; each compress/decompress call constructs its own
 * underlying compressor/decompressor so concurrent fan-out cannot race on internal scratch state.
 */
public sealed interface Codec
        permits Codec.Uncompressed,
                Codec.Snappy,
                Codec.Gzip,
                Codec.Deflate,
                Codec.Lz4Raw,
                Codec.Lz4Hadoop,
                Codec.Zstd,
                Codec.Brotli,
                Codec.Lzo,
                Codec.Bzip2,
                Codec.Xz {

    /** No-op codec; bytes pass through verbatim. */
    static Codec uncompressed() {
        return Uncompressed.INSTANCE;
    }

    /** Snappy raw block format (no framing). */
    static Codec snappy() {
        return Snappy.INSTANCE;
    }

    /** GZIP: DEFLATE wrapped in the gzip header/trailer (RFC 1952). */
    static Codec gzip() {
        return Gzip.INSTANCE;
    }

    /** Raw DEFLATE (RFC 1951, no zlib/gzip wrapper), as Avro's {@code deflate} OCF codec uses. */
    static Codec deflate() {
        return Deflate.INSTANCE;
    }

    /** LZ4 raw block format (no frame header). */
    static Codec lz4Raw() {
        return Lz4Raw.INSTANCE;
    }

    /** LZ4 with Hadoop stream framing. Decompression-only. */
    static Codec lz4Hadoop() {
        return Lz4Hadoop.INSTANCE;
    }

    /** ZSTD at the default compression level ({@value Zstd#DEFAULT_LEVEL}). */
    static Codec zstd() {
        return zstd(Zstd.DEFAULT_LEVEL);
    }

    /**
     * ZSTD at the supplied compression level.
     *
     * @param level zstd compression level; must be in {@code [1, 22]}
     */
    static Codec zstd(int level) {
        return new Zstd(level);
    }

    /** Brotli. Decompression-only: the {@code org.brotli:dec} library ships only a decoder. */
    static Codec brotli() {
        return Brotli.INSTANCE;
    }

    /** LZO raw block format (no Hadoop stream framing). */
    static Codec lzo() {
        return Lzo.INSTANCE;
    }

    /** bzip2 codec, as Avro's optional {@code bzip2} OCF codec uses. Stream-based via commons-compress. */
    static Codec bzip2() {
        return Bzip2.INSTANCE;
    }

    /** XZ container format (LZMA2), as Avro's optional {@code xz} OCF codec uses. Stream-based. */
    static Codec xz() {
        return Xz.INSTANCE;
    }

    /**
     * Decompresses the contents of {@code src} into {@code output}. The {@code output} segment must be at least as
     * large as the expected uncompressed payload. Returns the number of bytes written.
     */
    int decompress(MemorySegment src, MemorySegment output) throws IOException;

    /**
     * Decompresses the contents of {@code src} when the uncompressed size is not known up front, discovering it from
     * the compressed stream. Returns a read-only segment holding exactly the uncompressed bytes.
     *
     * @throws UnsupportedOperationException if this codec's wire format does not allow size discovery (raw LZ4, LZO)
     */
    MemorySegment decompress(MemorySegment src) throws IOException;

    /**
     * Compresses {@code src} into {@code output}. Returns the number of compressed bytes written.
     *
     * <p>Implementations must respect {@code output.byteSize()} as a hard upper bound; if compressed output would
     * exceed it, throw {@link IOException} -- callers size the output via {@link #maxCompressedLength(long)}.
     *
     * <p>{@code src} may be read-only. Some compression backends read their input through {@code Unsafe} and reject a
     * read-only segment; an implementation built on one of those must handle the rejection rather than pass it on.
     *
     * @throws UnsupportedOperationException if this codec only supports decompression (Brotli, Hadoop-framed LZ4)
     */
    int compress(MemorySegment src, MemorySegment output) throws IOException;

    /**
     * Maximum bytes the compressor might write for an input of length {@code uncompressedLength}.
     *
     * @throws UnsupportedOperationException if this codec only supports decompression (Brotli, Hadoop-framed LZ4)
     */
    long maxCompressedLength(long uncompressedLength);

    /** No-op codec; compress and decompress copy bytes verbatim. */
    record Uncompressed() implements Codec {

        static final Uncompressed INSTANCE = new Uncompressed();

        @Override
        public int decompress(MemorySegment src, MemorySegment output) {
            long size = src.byteSize();
            MemorySegment.copy(src, 0L, output, 0L, size);
            return (int) size;
        }

        @Override
        public MemorySegment decompress(MemorySegment src) {
            return src.asReadOnly();
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

    /**
     * Snappy raw block codec; uses aircompressor's snappy compressor/decompressor. The block format prefixes the
     * uncompressed length as a varint, which makes size-discovering decompression possible without framing.
     */
    record Snappy() implements Codec {

        static final Snappy INSTANCE = new Snappy();

        private static final int MAX_LENGTH_VARINT_BYTES = 5;

        @Override
        public int decompress(MemorySegment src, MemorySegment output) {
            return CompressionSupport.decompressReadingFrom(src, output, SnappyDecompressor.create()::decompress);
        }

        @Override
        public MemorySegment decompress(MemorySegment src) throws IOException {
            try {
                byte[] lengthPrefix = CompressionSupport.prefix(src, MAX_LENGTH_VARINT_BYTES);
                int uncompressedLength = SnappyDecompressor.create().getUncompressedLength(lengthPrefix, 0);
                if (uncompressedLength < 0) {
                    throw new IOException("Corrupt snappy block: negative uncompressed length " + uncompressedLength);
                }
                MemorySegment out = MemorySegment.ofArray(new byte[uncompressedLength]);
                decompress(src, out);
                return out.asReadOnly();
            } catch (MalformedInputException | IllegalArgumentException | IndexOutOfBoundsException e) {
                // The engine backends differ in how they report corrupt input; normalize to IOException.
                throw new IOException("Corrupt snappy block", e);
            }
        }

        @Override
        public int compress(MemorySegment src, MemorySegment output) throws IOException {
            SnappyCompressor compressor = SnappyCompressor.create();
            try {
                return CompressionSupport.compressReadingFrom(src, output, compressor::compress);
            } catch (IllegalArgumentException failure) {
                throw new IOException(
                        "SNAPPY compress failed for input of " + src.byteSize() + " bytes: " + failure.getMessage(),
                        failure);
            }
        }

        @Override
        public long maxCompressedLength(long uncompressedLength) {
            return SnappyCompressor.create().maxCompressedLength(CompressionSupport.intExact(uncompressedLength));
        }
    }

    /**
     * GZIP codec. The JDK's {@link GZIPInputStream}/{@link GZIPOutputStream} are stream/byte-array based; the boundary
     * with the codec's {@link MemorySegment} API is bridged via {@code byte[]} copies. Not on the hot path.
     */
    record Gzip() implements Codec {

        static final Gzip INSTANCE = new Gzip();

        @Override
        public int decompress(MemorySegment src, MemorySegment output) throws IOException {
            byte[] in = src.toArray(JAVA_BYTE);
            try (GZIPInputStream gz = new GZIPInputStream(new ByteArrayInputStream(in))) {
                return CompressionSupport.streamToSegment(gz, output);
            }
        }

        @Override
        public MemorySegment decompress(MemorySegment src) throws IOException {
            byte[] in = src.toArray(JAVA_BYTE);
            try (GZIPInputStream gz = new GZIPInputStream(new ByteArrayInputStream(in))) {
                return MemorySegment.ofArray(gz.readAllBytes()).asReadOnly();
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
            return CompressionSupport.deflateBound(uncompressedLength);
        }
    }

    /**
     * Raw DEFLATE codec (RFC 1951, no zlib/gzip wrapper). Decompression drives {@link Inflater} with {@link ByteBuffer}
     * views of the segments, never round-tripping through an intermediate {@code byte[]}.
     */
    record Deflate() implements Codec {

        static final Deflate INSTANCE = new Deflate();

        @Override
        public int decompress(MemorySegment src, MemorySegment output) throws IOException {
            try (Inflater inflater = new Inflater(/* nowrap= */ true)) {
                inflater.setInput(src.asByteBuffer());
                int written = 0;
                while (!inflater.finished()) {
                    ByteBuffer window =
                            output.asSlice(written, output.byteSize() - written).asByteBuffer();
                    int n = inflater.inflate(window);
                    if (n == 0) {
                        if (inflater.finished()) {
                            break;
                        }
                        if (inflater.needsInput() || inflater.needsDictionary()) {
                            throw new IOException("Truncated or corrupt deflate block");
                        }
                        throw new IOException("DEFLATE output buffer too small (limit=" + output.byteSize() + ")");
                    }
                    written += n;
                }
                return written;
            } catch (DataFormatException e) {
                throw new IOException("Corrupt deflate block", e);
            }
        }

        @Override
        public MemorySegment decompress(MemorySegment src) throws IOException {
            try (Inflater inflater = new Inflater(/* nowrap= */ true)) {
                inflater.setInput(src.asByteBuffer());
                return inflateAll(inflater, src.byteSize());
            }
        }

        private MemorySegment inflateAll(Inflater inflater, long compressedSize) throws IOException {
            int capacity = Math.max(64, (int) Math.min(Integer.MAX_VALUE, compressedSize * 4));
            MemorySegment target = MemorySegment.ofArray(new byte[capacity]);
            int produced = 0;
            try {
                while (!inflater.finished()) {
                    if (produced == capacity) {
                        capacity = grow(capacity);
                        MemorySegment grown = MemorySegment.ofArray(new byte[capacity]);
                        MemorySegment.copy(target, 0, grown, 0, produced);
                        target = grown;
                    }
                    ByteBuffer window =
                            target.asSlice(produced, capacity - produced).asByteBuffer();
                    int n = inflater.inflate(window);
                    if (n == 0) {
                        if (inflater.finished()) {
                            break;
                        }
                        if (inflater.needsInput() || inflater.needsDictionary()) {
                            throw new IOException("Truncated or corrupt deflate block");
                        }
                    }
                    produced += n;
                }
            } catch (DataFormatException e) {
                throw new IOException("Corrupt deflate block", e);
            }
            return target.asSlice(0, produced).asReadOnly();
        }

        private int grow(int capacity) throws IOException {
            long doubled = (long) capacity * 2;
            if (doubled > Integer.MAX_VALUE) {
                throw new IOException("Deflate block exceeds maximum size");
            }
            return (int) doubled;
        }

        @Override
        public int compress(MemorySegment src, MemorySegment output) throws IOException {
            CompressionSupport.BoundedSegmentOutputStream sink =
                    new CompressionSupport.BoundedSegmentOutputStream(output);
            Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, /* nowrap= */ true);
            try (DeflaterOutputStream out = new DeflaterOutputStream(sink, deflater)) {
                CompressionSupport.writeFromSegment(src, out);
            } finally {
                deflater.end();
            }
            return sink.written();
        }

        @Override
        public long maxCompressedLength(long uncompressedLength) {
            return CompressionSupport.deflateBound(uncompressedLength);
        }
    }

    /**
     * LZ4 raw block codec. Uses aircompressor's {@link Lz4Compressor} / {@link Lz4Decompressor}, which handle the raw
     * LZ4 block format with no frame header; the format does not record the uncompressed length, ruling out
     * size-discovering decompression.
     */
    record Lz4Raw() implements Codec {

        static final Lz4Raw INSTANCE = new Lz4Raw();

        @Override
        public int decompress(MemorySegment src, MemorySegment output) {
            return CompressionSupport.decompressReadingFrom(src, output, Lz4Decompressor.create()::decompress);
        }

        @Override
        public MemorySegment decompress(MemorySegment src) {
            throw new UnsupportedOperationException("Raw LZ4 blocks do not record the uncompressed length");
        }

        @Override
        public int compress(MemorySegment src, MemorySegment output) throws IOException {
            Lz4Compressor compressor = Lz4Compressor.create();
            try {
                return CompressionSupport.compressReadingFrom(src, output, compressor::compress);
            } catch (IllegalArgumentException failure) {
                throw new IOException(
                        "LZ4_RAW compress failed for input of " + src.byteSize() + " bytes: " + failure.getMessage(),
                        failure);
            }
        }

        @Override
        public long maxCompressedLength(long uncompressedLength) {
            return Lz4Compressor.create().maxCompressedLength(CompressionSupport.intExact(uncompressedLength));
        }
    }

    /**
     * LZ4 with Hadoop stream framing. Decompression-only: the framing has no aircompressor segment-level compressor and
     * no usable compressed-size bound; compress with {@link #lz4Raw()} instead.
     */
    record Lz4Hadoop() implements Codec {

        static final Lz4Hadoop INSTANCE = new Lz4Hadoop();

        private static final String COMPRESS_UNSUPPORTED_MESSAGE =
                "LZ4 Hadoop-framed compression is not supported; compress with lz4Raw() instead";

        @Override
        public int decompress(MemorySegment src, MemorySegment output) throws IOException {
            byte[] in = src.toArray(JAVA_BYTE);
            InputStream wrapped = new ByteArrayInputStream(in);
            try (HadoopInputStream hadoop = new Lz4HadoopStreams().createInputStream(wrapped)) {
                return CompressionSupport.streamToSegment(hadoop, output);
            }
        }

        @Override
        public MemorySegment decompress(MemorySegment src) throws IOException {
            byte[] in = src.toArray(JAVA_BYTE);
            InputStream wrapped = new ByteArrayInputStream(in);
            try (HadoopInputStream hadoop = new Lz4HadoopStreams().createInputStream(wrapped)) {
                return MemorySegment.ofArray(hadoop.readAllBytes()).asReadOnly();
            } catch (MalformedInputException e) {
                throw new IOException("Corrupt Hadoop-framed LZ4 stream", e);
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
     * ZSTD codec. The compression level (range {@code [1, 22]}, default {@value #DEFAULT_LEVEL}) is configured at
     * construction time. Size-discovering decompression reads the frame header's content size when present and falls
     * back to streaming decompression when the frame omits it (streaming writers usually do).
     */
    record Zstd(int level) implements Codec {

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
        public int decompress(MemorySegment src, MemorySegment output) {
            return CompressionSupport.decompressReadingFrom(src, output, ZstdDecompressor.create()::decompress);
        }

        @Override
        public MemorySegment decompress(MemorySegment src) throws IOException {
            byte[] in = src.toArray(JAVA_BYTE);
            try {
                long contentSize = ZstdDecompressor.create().getDecompressedSize(in, 0, in.length);
                if (contentSize > Integer.MAX_VALUE) {
                    throw new IOException("ZSTD frame content size exceeds maximum: " + contentSize);
                }
                if (contentSize >= 0) {
                    MemorySegment out = MemorySegment.ofArray(new byte[(int) contentSize]);
                    decompress(src, out);
                    return out.asReadOnly();
                }
                try (ZstdInputStream zstd = new ZstdInputStream(new ByteArrayInputStream(in))) {
                    return MemorySegment.ofArray(zstd.readAllBytes()).asReadOnly();
                }
            } catch (MalformedInputException | IllegalArgumentException | IndexOutOfBoundsException e) {
                // The engine backends differ in how they report corrupt input; normalize to IOException.
                throw new IOException("Corrupt zstd frame", e);
            }
        }

        @Override
        public int compress(MemorySegment src, MemorySegment output) throws IOException {
            ZstdCompressor compressor = compressorAtBestLevel();
            try {
                return CompressionSupport.compressReadingFrom(src, output, compressor::compress);
            } catch (IllegalArgumentException failure) {
                throw new IOException(
                        "ZSTD compress failed for input of " + src.byteSize() + " bytes: " + failure.getMessage(),
                        failure);
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
         * format; a payload written at the default level still reads back identically, on every platform.
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
     * Brotli codec. Decompression-only: the {@code org.brotli:dec} library on the classpath is decoder-only and
     * aircompressor v3 does not ship a Brotli compressor.
     */
    record Brotli() implements Codec {

        static final Brotli INSTANCE = new Brotli();

        private static final String COMPRESS_UNSUPPORTED_MESSAGE =
                "Brotli compression is not supported: the org.brotli:dec dependency is decoder-only "
                        + "and aircompressor v3 does not provide a Brotli compressor";

        @Override
        public int decompress(MemorySegment src, MemorySegment output) throws IOException {
            byte[] in = src.toArray(JAVA_BYTE);
            try (BrotliInputStream brotli = new BrotliInputStream(new ByteArrayInputStream(in))) {
                return CompressionSupport.streamToSegment(brotli, output);
            }
        }

        @Override
        public MemorySegment decompress(MemorySegment src) throws IOException {
            byte[] in = src.toArray(JAVA_BYTE);
            try (BrotliInputStream brotli = new BrotliInputStream(new ByteArrayInputStream(in))) {
                return MemorySegment.ofArray(brotli.readAllBytes()).asReadOnly();
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
     * stream framing); the format does not record the uncompressed length, ruling out size-discovering decompression.
     */
    record Lzo() implements Codec {

        static final Lzo INSTANCE = new Lzo();

        @Override
        public int decompress(MemorySegment src, MemorySegment output) {
            return CompressionSupport.decompressReadingFrom(src, output, new LzoDecompressor()::decompress);
        }

        @Override
        public MemorySegment decompress(MemorySegment src) {
            throw new UnsupportedOperationException("Raw LZO blocks do not record the uncompressed length");
        }

        @Override
        public int compress(MemorySegment src, MemorySegment output) throws IOException {
            LzoCompressor compressor = new LzoCompressor();
            try {
                return CompressionSupport.compressReadingFrom(src, output, compressor::compress);
            } catch (IllegalArgumentException failure) {
                throw new IOException(
                        "LZO compress failed for input of " + src.byteSize() + " bytes: " + failure.getMessage(),
                        failure);
            }
        }

        @Override
        public long maxCompressedLength(long uncompressedLength) {
            return new LzoCompressor().maxCompressedLength(CompressionSupport.intExact(uncompressedLength));
        }
    }

    /** bzip2 codec. The worst-case bound follows the bzip2 manual: about 1% growth plus 600 bytes. */
    record Bzip2() implements Codec {

        static final Bzip2 INSTANCE = new Bzip2();

        @Override
        public int decompress(MemorySegment src, MemorySegment output) throws IOException {
            byte[] in = src.toArray(JAVA_BYTE);
            try (BZip2CompressorInputStream bzip2 = new BZip2CompressorInputStream(new ByteArrayInputStream(in))) {
                return CompressionSupport.streamToSegment(bzip2, output);
            }
        }

        @Override
        public MemorySegment decompress(MemorySegment src) throws IOException {
            byte[] in = src.toArray(JAVA_BYTE);
            try (BZip2CompressorInputStream bzip2 = new BZip2CompressorInputStream(new ByteArrayInputStream(in))) {
                return MemorySegment.ofArray(bzip2.readAllBytes()).asReadOnly();
            }
        }

        @Override
        public int compress(MemorySegment src, MemorySegment output) throws IOException {
            CompressionSupport.BoundedSegmentOutputStream sink =
                    new CompressionSupport.BoundedSegmentOutputStream(output);
            try (BZip2CompressorOutputStream bzip2 = new BZip2CompressorOutputStream(sink)) {
                CompressionSupport.writeFromSegment(src, bzip2);
            }
            return sink.written();
        }

        @Override
        public long maxCompressedLength(long uncompressedLength) {
            long n = CompressionSupport.intExact(uncompressedLength);
            return n + (n / 100) + 600;
        }
    }

    /** XZ codec. The worst-case bound is generous: LZMA2 worst case is well under 1.34x plus a fixed header. */
    record Xz() implements Codec {

        static final Xz INSTANCE = new Xz();

        // Bounds the dictionary allocation a crafted stream can declare; 96 MiB covers preset 9's 64 MiB
        // dictionary plus decoder overhead, and MemoryLimitException is an IOException subclass.
        private static final int XZ_DECODE_MEMLIMIT_KIB = 98_304;

        @Override
        public int decompress(MemorySegment src, MemorySegment output) throws IOException {
            byte[] in = src.toArray(JAVA_BYTE);
            try (XZInputStream xz = new XZInputStream(new ByteArrayInputStream(in), XZ_DECODE_MEMLIMIT_KIB)) {
                return CompressionSupport.streamToSegment(xz, output);
            }
        }

        @Override
        public MemorySegment decompress(MemorySegment src) throws IOException {
            byte[] in = src.toArray(JAVA_BYTE);
            try (XZInputStream xz = new XZInputStream(new ByteArrayInputStream(in), XZ_DECODE_MEMLIMIT_KIB)) {
                return MemorySegment.ofArray(xz.readAllBytes()).asReadOnly();
            }
        }

        @Override
        public int compress(MemorySegment src, MemorySegment output) throws IOException {
            CompressionSupport.BoundedSegmentOutputStream sink =
                    new CompressionSupport.BoundedSegmentOutputStream(output);
            try (XZOutputStream xz = new XZOutputStream(sink, new LZMA2Options(LZMA2Options.PRESET_DEFAULT))) {
                CompressionSupport.writeFromSegment(src, xz);
            }
            return sink.written();
        }

        @Override
        public long maxCompressedLength(long uncompressedLength) {
            long n = CompressionSupport.intExact(uncompressedLength);
            return n + (n / 3) + 256;
        }
    }
}
