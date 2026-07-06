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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.tileverse.parquetry.format.CompressionCodec;

import io.airlift.compress.v3.hadoop.HadoopOutputStream;
import io.airlift.compress.v3.lz4.Lz4Compressor;
import io.airlift.compress.v3.lz4.Lz4HadoopStreams;
import io.airlift.compress.v3.lzo.LzoCompressor;
import io.airlift.compress.v3.snappy.SnappyCompressor;
import io.airlift.compress.v3.zstd.ZstdCompressor;

/**
 * Coverage for the {@link Compression} sealed type.
 *
 * <ul>
 *   <li>{@link SealedTypes} - factory / wire-codec mapping / sealed-permits checks.
 *   <li>{@link Compress} - compress-side round-trip, level handling, overflow, Brotli decode-only.
 *   <li>{@link RoundTrip} - decompress-side parameterized payloads, fed by external-compressor fixtures.
 * </ul>
 */
class CompressionTest {

    @Nested
    class SealedTypes {

        @Test
        void uncompressedMapsToUncompressedWireCode() {
            Compression codec = Compression.uncompressed();

            assertThat(codec).isInstanceOf(Compression.Uncompressed.class);
            assertThat(codec.wireCodec()).isEqualTo(CompressionCodec.UNCOMPRESSED);
        }

        @Test
        void snappyMapsToSnappyWireCode() {
            Compression codec = Compression.snappy();

            assertThat(codec).isInstanceOf(Compression.Snappy.class);
            assertThat(codec.wireCodec()).isEqualTo(CompressionCodec.SNAPPY);
        }

        @Test
        void gzipMapsToGzipWireCode() {
            Compression codec = Compression.gzip();

            assertThat(codec).isInstanceOf(Compression.Gzip.class);
            assertThat(codec.wireCodec()).isEqualTo(CompressionCodec.GZIP);
        }

        @Test
        void lz4RawMapsToLz4RawWireCode() {
            Compression codec = Compression.lz4Raw();

            assertThat(codec).isInstanceOf(Compression.Lz4Raw.class);
            assertThat(codec.wireCodec()).isEqualTo(CompressionCodec.LZ4_RAW);
        }

        @Test
        void brotliMapsToBrotliWireCode() {
            Compression codec = Compression.brotli();

            assertThat(codec).isInstanceOf(Compression.Brotli.class);
            assertThat(codec.wireCodec()).isEqualTo(CompressionCodec.BROTLI);
        }

        @Test
        void lzoMapsToLzoWireCode() {
            Compression codec = Compression.lzo();

            assertThat(codec).isInstanceOf(Compression.Lzo.class);
            assertThat(codec.wireCodec()).isEqualTo(CompressionCodec.LZO);
        }

        @Test
        void lz4HadoopMapsToLegacyLz4WireCode() {
            Compression codec = Compression.lz4Hadoop();

            assertThat(codec).isInstanceOf(Compression.Lz4Hadoop.class);
            assertThat(codec.wireCodec()).isEqualTo(CompressionCodec.LZ4);
        }

        @ParameterizedTest
        @ValueSource(ints = {1, 3, 9, 22})
        void zstdAcceptsValidLevels(int level) {
            Compression codec = Compression.zstd(level);

            assertThat(codec).isInstanceOf(Compression.Zstd.class);
            Compression.Zstd zstd = (Compression.Zstd) codec;
            assertThat(zstd.level()).isEqualTo(level);
            assertThat(zstd.wireCodec()).isEqualTo(CompressionCodec.ZSTD);
        }

        @ParameterizedTest
        @ValueSource(ints = {Integer.MIN_VALUE, -1, 0, 23, 100})
        void zstdRejectsOutOfRangeLevels(int bad) {
            assertThatThrownBy(() -> Compression.zstd(bad))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(Integer.toString(bad));
        }

        @Test
        void sealedPermitsClosure() {
            Class<?>[] permitted = Compression.class.getPermittedSubclasses();

            assertThat(permitted)
                    .containsExactlyInAnyOrder(
                            Compression.Uncompressed.class,
                            Compression.Snappy.class,
                            Compression.Gzip.class,
                            Compression.Lz4Raw.class,
                            Compression.Zstd.class,
                            Compression.Brotli.class,
                            Compression.Lzo.class,
                            Compression.Lz4Hadoop.class);
            assertThat(Compression.class.isSealed()).isTrue();
        }

        @Test
        void forWireCodecResolvesEveryWireVariant() {
            assertThat(Compression.forWireCodec(CompressionCodec.UNCOMPRESSED))
                    .isInstanceOf(Compression.Uncompressed.class);
            assertThat(Compression.forWireCodec(CompressionCodec.SNAPPY)).isInstanceOf(Compression.Snappy.class);
            assertThat(Compression.forWireCodec(CompressionCodec.GZIP)).isInstanceOf(Compression.Gzip.class);
            assertThat(Compression.forWireCodec(CompressionCodec.LZ4_RAW)).isInstanceOf(Compression.Lz4Raw.class);
            assertThat(Compression.forWireCodec(CompressionCodec.BROTLI)).isInstanceOf(Compression.Brotli.class);
            assertThat(Compression.forWireCodec(CompressionCodec.LZO)).isInstanceOf(Compression.Lzo.class);
            assertThat(Compression.forWireCodec(CompressionCodec.LZ4)).isInstanceOf(Compression.Lz4Hadoop.class);
        }

        @Test
        void forWireCodecDefaultsZstdLevelToThree() {
            Compression codec = Compression.forWireCodec(CompressionCodec.ZSTD);
            assertThat(codec).isInstanceOf(Compression.Zstd.class);
            assertThat(((Compression.Zstd) codec).level()).isEqualTo(Compression.Zstd.DEFAULT_LEVEL);
        }

        @Test
        void lz4HadoopRefusesCompression() {
            Compression codec = Compression.lz4Hadoop();
            assertThatThrownBy(() -> codec.compress(MemorySegment.NULL, MemorySegment.NULL))
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("deprecates");
        }

        @Test
        void uncompressedDecompressIsIdentity() throws Exception {
            Compression codec = Compression.uncompressed();
            byte[] data = "hello".getBytes();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment out = arena.allocate(data.length);
                codec.decompress(MemorySegment.ofBuffer(ByteBuffer.wrap(data)), out);
                assertThat(out.toArray(JAVA_BYTE)).isEqualTo(data);
            }
        }
    }

    /**
     * Compress-side round-trip coverage. Brotli is deliberately omitted: the {@code org.brotli:dec} library on the
     * classpath is decoder-only, so {@link Compression.Brotli#compress(MemorySegment, MemorySegment)} throws
     * {@link UnsupportedOperationException} -- see {@link #brotliCompressIsUnsupported()}.
     */
    @Nested
    class Compress {

        private static final byte[] COMPRESSIBLE_PAYLOAD =
                "the quick brown fox jumps over the lazy dog ".repeat(64).getBytes();

        @ParameterizedTest(name = "{0}")
        @MethodSource("compressibleCodecs")
        void roundTripsCompressiblePayload(Compression codec) throws Exception {
            byte[] payload = COMPRESSIBLE_PAYLOAD;

            try (Arena arena = Arena.ofConfined()) {
                MemorySegment src = arena.allocate(payload.length);
                MemorySegment.copy(payload, 0, src, ValueLayout.JAVA_BYTE, 0, payload.length);

                long maxLen = codec.maxCompressedLength(payload.length);
                MemorySegment compressed = arena.allocate(maxLen);
                int compressedLen = codec.compress(src, compressed);
                assertThat(compressedLen).as("compressed length").isPositive().isLessThanOrEqualTo((int) maxLen);

                MemorySegment decompressed = arena.allocate(payload.length);
                int decompressedLen = codec.decompress(compressed.asSlice(0, compressedLen), decompressed);
                assertThat(decompressedLen).as("decompressed length").isEqualTo(payload.length);

                byte[] back = new byte[payload.length];
                MemorySegment.copy(decompressed, ValueLayout.JAVA_BYTE, 0, back, 0, payload.length);
                assertThat(back).as("round-tripped bytes").isEqualTo(payload);
            }
        }

        static List<Compression> compressibleCodecs() {
            return List.of(
                    Compression.uncompressed(),
                    Compression.snappy(),
                    Compression.gzip(),
                    Compression.lz4Raw(),
                    Compression.zstd(3));
        }

        @Test
        void zstdAcceptsLevelAndHigherLevelProducesSmallerOrEqualOutput() throws Exception {
            byte[] payload = "abcdefghij".repeat(1024).getBytes();
            Compression level1 = Compression.zstd(1);
            Compression level22 = Compression.zstd(22);

            try (Arena arena = Arena.ofConfined()) {
                MemorySegment src = arena.allocate(payload.length);
                MemorySegment.copy(payload, 0, src, ValueLayout.JAVA_BYTE, 0, payload.length);

                MemorySegment outLow = arena.allocate(level1.maxCompressedLength(payload.length));
                int lowLen = level1.compress(src, outLow);

                MemorySegment outHigh = arena.allocate(level22.maxCompressedLength(payload.length));
                int highLen = level22.compress(src, outHigh);

                assertThat(highLen).as("level 22 output size").isLessThanOrEqualTo(lowLen);

                MemorySegment back = arena.allocate(payload.length);
                int backLen = level22.decompress(outHigh.asSlice(0, highLen), back);
                assertThat(backLen).isEqualTo(payload.length);

                byte[] decoded = new byte[payload.length];
                MemorySegment.copy(back, ValueLayout.JAVA_BYTE, 0, decoded, 0, payload.length);
                assertThat(decoded).isEqualTo(payload);
            }
        }

        @Test
        void zstdRejectsLevelsOutsideValidRange() {
            assertThatThrownBy(() -> Compression.zstd(0)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> Compression.zstd(23)).isInstanceOf(IllegalArgumentException.class);
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("nonTrivialCompressibleCodecs")
        void compressOverflowThrowsIOException(Compression codec) {
            try (Arena arena = Arena.ofConfined()) {
                byte[] payload = new byte[4096];
                // Random-like fill so even fast codecs cannot shrink it into 1 byte.
                for (int i = 0; i < payload.length; i++) {
                    payload[i] = (byte) (i * 31);
                }
                MemorySegment src = arena.allocate(payload.length);
                MemorySegment.copy(payload, 0, src, ValueLayout.JAVA_BYTE, 0, payload.length);
                MemorySegment tiny = arena.allocate(1);

                assertThatThrownBy(() -> codec.compress(src, tiny)).isInstanceOf(IOException.class);
            }
        }

        /**
         * The four real compressors whose overflow path is worth exercising. {@code UNCOMPRESSED} is excluded because
         * its overflow path is trivially the same as the IOException-on-truncation contract.
         */
        static List<Compression> nonTrivialCompressibleCodecs() {
            return List.of(Compression.snappy(), Compression.gzip(), Compression.lz4Raw(), Compression.zstd(3));
        }

        @Test
        void brotliCompressIsUnsupported() {
            Compression codec = Compression.brotli();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment src = arena.allocate(16);
                MemorySegment out = arena.allocate(64);
                assertThatThrownBy(() -> codec.compress(src, out)).isInstanceOf(UnsupportedOperationException.class);
                assertThatThrownBy(() -> codec.maxCompressedLength(16))
                        .isInstanceOf(UnsupportedOperationException.class);
            }
        }

        @Test
        void gzipMaxCompressedLengthRejectsOutOfRangeInputs() {
            Compression gzip = Compression.gzip();
            assertThatThrownBy(() -> gzip.maxCompressedLength(-1L)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> gzip.maxCompressedLength((long) Integer.MAX_VALUE + 1L))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    /**
     * Round-trip checks for each {@link Compression} against a representative set of byte payloads: short edge cases,
     * medium and large buffers, all-zero, all-0xFF, alternating bits, ASCII text, and a fixed-seed pseudo-random
     * buffer.
     */
    @Nested
    class RoundTrip {

        static Stream<Arguments> payloads() {
            return Stream.of(
                    Arguments.of("singleByte", new byte[] {0x42}),
                    Arguments.of("twoBytes", new byte[] {0x01, (byte) 0xff}),
                    Arguments.of("zeros16", new byte[16]),
                    Arguments.of("zeros4096", new byte[4096]),
                    Arguments.of("ones16", fillBytes(16, (byte) 0xff)),
                    Arguments.of("ones4096", fillBytes(4096, (byte) 0xff)),
                    Arguments.of("alternating64", alternating(64)),
                    Arguments.of("ascii", "the quick brown fox jumps over the lazy dog 0123456789".getBytes()),
                    Arguments.of("random256", random(256, 0xCAFEBABEL)),
                    Arguments.of("random4096", random(4096, 0xDEADBEEFL)));
        }

        @ParameterizedTest(name = "snappy/{0}")
        @MethodSource("payloads")
        void snappyRoundTrip(String name, byte[] original) throws Exception {
            SnappyCompressor compressor = SnappyCompressor.create();
            byte[] compressed = new byte[compressor.maxCompressedLength(original.length)];
            int compressedLen = compressor.compress(original, 0, original.length, compressed, 0, compressed.length);

            Compression codec = Compression.snappy();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment out = arena.allocate(original.length);
                codec.decompress(MemorySegment.ofBuffer(ByteBuffer.wrap(compressed, 0, compressedLen)), out);
                assertThat(out.toArray(JAVA_BYTE)).isEqualTo(original);
            }
        }

        @ParameterizedTest(name = "zstd/{0}")
        @MethodSource("payloads")
        void zstdRoundTrip(String name, byte[] original) throws Exception {
            ZstdCompressor compressor = ZstdCompressor.create();
            byte[] compressed = new byte[compressor.maxCompressedLength(original.length)];
            int compressedLen = compressor.compress(original, 0, original.length, compressed, 0, compressed.length);

            Compression codec = Compression.zstd(3);
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment out = arena.allocate(original.length);
                codec.decompress(MemorySegment.ofBuffer(ByteBuffer.wrap(compressed, 0, compressedLen)), out);
                assertThat(out.toArray(JAVA_BYTE)).isEqualTo(original);
            }
        }

        @ParameterizedTest(name = "lz4raw/{0}")
        @MethodSource("payloads")
        void lz4RawRoundTrip(String name, byte[] original) throws Exception {
            Lz4Compressor compressor = Lz4Compressor.create();
            byte[] compressed = new byte[compressor.maxCompressedLength(original.length)];
            int compressedLen = compressor.compress(original, 0, original.length, compressed, 0, compressed.length);

            Compression codec = Compression.lz4Raw();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment out = arena.allocate(original.length);
                codec.decompress(MemorySegment.ofBuffer(ByteBuffer.wrap(compressed, 0, compressedLen)), out);
                assertThat(out.toArray(JAVA_BYTE)).isEqualTo(original);
            }
        }

        @ParameterizedTest(name = "gzip/{0}")
        @MethodSource("payloads")
        void gzipRoundTrip(String name, byte[] original) throws Exception {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try (GZIPOutputStream gz = new GZIPOutputStream(bos)) {
                gz.write(original);
            }
            byte[] compressed = bos.toByteArray();

            Compression codec = Compression.gzip();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment out = arena.allocate(original.length);
                codec.decompress(MemorySegment.ofBuffer(ByteBuffer.wrap(compressed)), out);
                assertThat(out.toArray(JAVA_BYTE)).isEqualTo(original);
            }
        }

        @ParameterizedTest(name = "lzo/{0}")
        @MethodSource("payloads")
        void lzoRoundTrip(String name, byte[] original) throws Exception {
            LzoCompressor compressor = new LzoCompressor();
            byte[] compressed = new byte[compressor.maxCompressedLength(original.length)];
            int compressedLen = compressor.compress(original, 0, original.length, compressed, 0, compressed.length);

            Compression codec = Compression.lzo();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment out = arena.allocate(original.length);
                codec.decompress(MemorySegment.ofBuffer(ByteBuffer.wrap(compressed, 0, compressedLen)), out);
                assertThat(out.toArray(JAVA_BYTE)).isEqualTo(original);
            }
        }

        @ParameterizedTest(name = "lz4Hadoop/{0}")
        @MethodSource("payloads")
        void lz4HadoopDecodeOnly(String name, byte[] original) throws Exception {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try (HadoopOutputStream framed = new Lz4HadoopStreams().createOutputStream(bos)) {
                framed.write(original);
            }
            byte[] compressed = bos.toByteArray();

            Compression codec = Compression.lz4Hadoop();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment out = arena.allocate(original.length);
                codec.decompress(MemorySegment.ofBuffer(ByteBuffer.wrap(compressed)), out);
                assertThat(out.toArray(JAVA_BYTE)).isEqualTo(original);
            }
        }

        /**
         * The deprecated LZ4 codec is ambiguous: parquet-cpp writers emit raw LZ4 blocks with no Hadoop frame under the
         * same wire code that parquet-mr uses for Hadoop-framed LZ4. The reader must accept both. Here the payload is
         * raw-LZ4 compressed (no frame) and decoded through {@link Compression#lz4Hadoop()}, which has to fall back to
         * raw decoding.
         */
        @ParameterizedTest(name = "lz4HadoopRawFallback/{0}")
        @MethodSource("payloads")
        void lz4HadoopFallsBackToRawLz4Blocks(String name, byte[] original) throws Exception {
            Lz4Compressor compressor = Lz4Compressor.create();
            byte[] compressed = new byte[compressor.maxCompressedLength(original.length)];
            int compressedLen = compressor.compress(original, 0, original.length, compressed, 0, compressed.length);

            Compression codec = Compression.lz4Hadoop();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment out = arena.allocate(original.length);
                codec.decompress(MemorySegment.ofBuffer(ByteBuffer.wrap(compressed, 0, compressedLen)), out);
                assertThat(out.toArray(JAVA_BYTE)).isEqualTo(original);
            }
        }

        /**
         * Brotli round-trip using a hardcoded fixture. The org.brotli:dec library is decoder-only; there is no encoder
         * available on the classpath. The fixture was produced by: {@code echo -n "hello world" | brotli --stdout | xxd
         * -i}
         */
        @Test
        void brotliFixture() throws Exception {
            byte[] compressed = {
                (byte) 0x0f, (byte) 0x05, (byte) 0x80, (byte) 0x68, (byte) 0x65,
                (byte) 0x6c, (byte) 0x6c, (byte) 0x6f, (byte) 0x20, (byte) 0x77,
                (byte) 0x6f, (byte) 0x72, (byte) 0x6c, (byte) 0x64, (byte) 0x03
            };
            byte[] expected = "hello world".getBytes();

            Compression codec = Compression.brotli();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment out = arena.allocate(expected.length);
                codec.decompress(MemorySegment.ofBuffer(ByteBuffer.wrap(compressed)), out);
                assertThat(out.toArray(JAVA_BYTE)).isEqualTo(expected);
            }
        }

        private static byte[] fillBytes(int length, byte value) {
            byte[] result = new byte[length];
            Arrays.fill(result, value);
            return result;
        }

        private static byte[] alternating(int length) {
            byte[] result = new byte[length];
            for (int i = 0; i < length; i++) {
                result[i] = (byte) (i % 2 == 0 ? 0xaa : 0x55);
            }
            return result;
        }

        private static byte[] random(int length, long seed) {
            byte[] result = new byte[length];
            new Random(seed).nextBytes(result);
            return result;
        }
    }
}
