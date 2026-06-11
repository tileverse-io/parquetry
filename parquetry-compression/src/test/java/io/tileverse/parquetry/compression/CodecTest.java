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
package io.tileverse.parquetry.compression;

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

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.airlift.compress.v3.hadoop.HadoopOutputStream;
import io.airlift.compress.v3.lz4.Lz4HadoopStreams;
import io.airlift.compress.v3.zstd.ZstdOutputStream;

/**
 * Coverage for the {@link Codec} sealed type.
 *
 * <ul>
 *   <li>{@link SealedTypes} - factory / sealed-permits / level-validation checks.
 *   <li>{@link Compress} - compress-side round-trip, overflow, decode-only codecs.
 *   <li>{@link SizedDecompress} - decompression into a caller-sized output across payloads.
 *   <li>{@link SizeDiscoveringDecompress} - the {@code decompress(src)} shape for codecs whose stream reveals the
 *       uncompressed size, including zstd frames that omit the content size.
 * </ul>
 */
class CodecTest {

    private static final byte[] COMPRESSIBLE_PAYLOAD =
            "the quick brown fox jumps over the lazy dog ".repeat(64).getBytes();

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

    @Nested
    class SealedTypes {

        @Test
        void sealedPermitsClosure() {
            Class<?>[] permitted = Codec.class.getPermittedSubclasses();

            assertThat(permitted)
                    .containsExactlyInAnyOrder(
                            Codec.Uncompressed.class,
                            Codec.Snappy.class,
                            Codec.Gzip.class,
                            Codec.Deflate.class,
                            Codec.Lz4Raw.class,
                            Codec.Lz4Hadoop.class,
                            Codec.Zstd.class,
                            Codec.Brotli.class,
                            Codec.Lzo.class,
                            Codec.Bzip2.class,
                            Codec.Xz.class);
            assertThat(Codec.class.isSealed()).isTrue();
        }

        @ParameterizedTest
        @ValueSource(ints = {1, 3, 9, 22})
        void zstdAcceptsValidLevels(int level) {
            Codec codec = Codec.zstd(level);

            assertThat(codec).isInstanceOf(Codec.Zstd.class);
            assertThat(((Codec.Zstd) codec).level()).isEqualTo(level);
        }

        @ParameterizedTest
        @ValueSource(ints = {Integer.MIN_VALUE, -1, 0, 23, 100})
        void zstdRejectsOutOfRangeLevels(int bad) {
            assertThatThrownBy(() -> Codec.zstd(bad))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(Integer.toString(bad));
        }

        @Test
        void zstdNoArgFactoryUsesDefaultLevel() {
            Codec codec = Codec.zstd();
            assertThat(((Codec.Zstd) codec).level()).isEqualTo(Codec.Zstd.DEFAULT_LEVEL);
        }
    }

    @Nested
    class Compress {

        @ParameterizedTest(name = "{0}")
        @MethodSource("compressibleCodecs")
        void roundTripsCompressiblePayload(Codec codec) throws Exception {
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

        static List<Codec> compressibleCodecs() {
            return List.of(
                    Codec.uncompressed(),
                    Codec.snappy(),
                    Codec.gzip(),
                    Codec.deflate(),
                    Codec.lz4Raw(),
                    Codec.zstd(3),
                    Codec.lzo(),
                    Codec.bzip2(),
                    Codec.xz());
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource("nonTrivialCompressibleCodecs")
        void compressOverflowThrowsIOException(Codec codec) {
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

        static List<Codec> nonTrivialCompressibleCodecs() {
            return List.of(
                    Codec.snappy(),
                    Codec.gzip(),
                    Codec.deflate(),
                    Codec.lz4Raw(),
                    Codec.zstd(3),
                    Codec.lzo(),
                    Codec.bzip2(),
                    Codec.xz());
        }

        @Test
        void zstdHigherLevelProducesSmallerOrEqualOutput() throws Exception {
            byte[] payload = "abcdefghij".repeat(1024).getBytes();
            Codec level1 = Codec.zstd(1);
            Codec level22 = Codec.zstd(22);

            try (Arena arena = Arena.ofConfined()) {
                MemorySegment src = arena.allocate(payload.length);
                MemorySegment.copy(payload, 0, src, ValueLayout.JAVA_BYTE, 0, payload.length);

                MemorySegment outLow = arena.allocate(level1.maxCompressedLength(payload.length));
                int lowLen = level1.compress(src, outLow);

                MemorySegment outHigh = arena.allocate(level22.maxCompressedLength(payload.length));
                int highLen = level22.compress(src, outHigh);

                assertThat(highLen).as("level 22 output size").isLessThanOrEqualTo(lowLen);
            }
        }

        @Test
        void brotliCompressIsUnsupported() {
            Codec codec = Codec.brotli();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment src = arena.allocate(16);
                MemorySegment out = arena.allocate(64);
                assertThatThrownBy(() -> codec.compress(src, out)).isInstanceOf(UnsupportedOperationException.class);
                assertThatThrownBy(() -> codec.maxCompressedLength(16))
                        .isInstanceOf(UnsupportedOperationException.class);
            }
        }

        @Test
        void lz4HadoopCompressIsUnsupported() {
            Codec codec = Codec.lz4Hadoop();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment src = arena.allocate(16);
                MemorySegment out = arena.allocate(64);
                assertThatThrownBy(() -> codec.compress(src, out))
                        .isInstanceOf(UnsupportedOperationException.class)
                        .hasMessageContaining("lz4Raw");
                assertThatThrownBy(() -> codec.maxCompressedLength(16))
                        .isInstanceOf(UnsupportedOperationException.class);
            }
        }

        @Test
        void deflateBoundRejectsOutOfRangeInputs() {
            Codec deflate = Codec.deflate();
            assertThatThrownBy(() -> deflate.maxCompressedLength(-1L)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> deflate.maxCompressedLength((long) Integer.MAX_VALUE + 1L))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    class SizedDecompress {

        @ParameterizedTest(name = "lz4Hadoop/{0}")
        @MethodSource("io.tileverse.parquetry.compression.CodecTest#payloads")
        void lz4HadoopDecodesFramedStreams(String name, byte[] original) throws Exception {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try (HadoopOutputStream framed = new Lz4HadoopStreams().createOutputStream(bos)) {
                framed.write(original);
            }
            byte[] compressed = bos.toByteArray();

            Codec codec = Codec.lz4Hadoop();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment out = arena.allocate(original.length);
                codec.decompress(MemorySegment.ofBuffer(ByteBuffer.wrap(compressed)), out);
                assertThat(out.toArray(JAVA_BYTE)).isEqualTo(original);
            }
        }

        /**
         * Brotli decode using a hardcoded fixture; the classpath has no Brotli encoder. Produced by: {@code echo -n
         * "hello world" | brotli --stdout | xxd -i}
         */
        @Test
        void brotliFixture() throws Exception {
            byte[] compressed = brotliHelloWorld();
            byte[] expected = "hello world".getBytes();

            Codec codec = Codec.brotli();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment out = arena.allocate(expected.length);
                codec.decompress(MemorySegment.ofBuffer(ByteBuffer.wrap(compressed)), out);
                assertThat(out.toArray(JAVA_BYTE)).isEqualTo(expected);
            }
        }

        @Test
        void deflateRejectsTooSmallOutput() throws Exception {
            byte[] payload = COMPRESSIBLE_PAYLOAD;
            Codec deflate = Codec.deflate();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment src = arena.allocate(payload.length);
                MemorySegment.copy(payload, 0, src, ValueLayout.JAVA_BYTE, 0, payload.length);
                MemorySegment compressed = arena.allocate(deflate.maxCompressedLength(payload.length));
                int compressedLen = deflate.compress(src, compressed);
                MemorySegment compressedSlice = compressed.asSlice(0, compressedLen);

                MemorySegment tiny = arena.allocate(payload.length / 2);
                assertThatThrownBy(() -> deflate.decompress(compressedSlice, tiny))
                        .isInstanceOf(IOException.class);
            }
        }
    }

    @Nested
    class SizeDiscoveringDecompress {

        @ParameterizedTest(name = "{0}")
        @MethodSource("sizeDiscoveringCodecs")
        void roundTripsWithoutKnownSize(Codec codec) throws Exception {
            byte[] payload = COMPRESSIBLE_PAYLOAD;
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment src = arena.allocate(payload.length);
                MemorySegment.copy(payload, 0, src, ValueLayout.JAVA_BYTE, 0, payload.length);

                MemorySegment compressed = arena.allocate(codec.maxCompressedLength(payload.length));
                int compressedLen = codec.compress(src, compressed);

                MemorySegment back = codec.decompress(compressed.asSlice(0, compressedLen));
                assertThat(back.isReadOnly()).isTrue();
                assertThat(back.toArray(JAVA_BYTE)).isEqualTo(payload);
            }
        }

        static List<Codec> sizeDiscoveringCodecs() {
            return List.of(
                    Codec.uncompressed(),
                    Codec.snappy(),
                    Codec.gzip(),
                    Codec.deflate(),
                    Codec.zstd(3),
                    Codec.bzip2(),
                    Codec.xz());
        }

        @ParameterizedTest(name = "deflate/{0}")
        @MethodSource("io.tileverse.parquetry.compression.CodecTest#payloads")
        void deflateDiscoversSizeAcrossPayloads(String name, byte[] original) throws Exception {
            Codec deflate = Codec.deflate();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment src = arena.allocate(original.length);
                MemorySegment.copy(original, 0, src, ValueLayout.JAVA_BYTE, 0, original.length);
                MemorySegment compressed = arena.allocate(deflate.maxCompressedLength(original.length));
                int compressedLen = deflate.compress(src, compressed);

                MemorySegment back = deflate.decompress(compressed.asSlice(0, compressedLen));
                assertThat(back.toArray(JAVA_BYTE)).isEqualTo(original);
            }
        }

        /**
         * Streaming zstd writers omit the frame content size (it is unknown when the frame starts); the
         * size-discovering shape must fall back to streaming decompression for such frames.
         */
        @ParameterizedTest(name = "zstdStreamed/{0}")
        @MethodSource("io.tileverse.parquetry.compression.CodecTest#payloads")
        void zstdDecodesFramesWithoutContentSize(String name, byte[] original) throws Exception {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try (ZstdOutputStream zstd = new ZstdOutputStream(bos)) {
                zstd.write(original);
            }
            byte[] compressed = bos.toByteArray();

            MemorySegment back = Codec.zstd().decompress(MemorySegment.ofBuffer(ByteBuffer.wrap(compressed)));
            assertThat(back.toArray(JAVA_BYTE)).isEqualTo(original);
        }

        @Test
        void brotliDiscoversSize() throws Exception {
            MemorySegment back = Codec.brotli().decompress(MemorySegment.ofBuffer(ByteBuffer.wrap(brotliHelloWorld())));
            assertThat(back.toArray(JAVA_BYTE)).isEqualTo("hello world".getBytes());
        }

        @Test
        void lz4HadoopDiscoversSize() throws Exception {
            byte[] original = COMPRESSIBLE_PAYLOAD;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try (HadoopOutputStream framed = new Lz4HadoopStreams().createOutputStream(bos)) {
                framed.write(original);
            }
            MemorySegment back =
                    Codec.lz4Hadoop().decompress(MemorySegment.ofBuffer(ByteBuffer.wrap(bos.toByteArray())));
            assertThat(back.toArray(JAVA_BYTE)).isEqualTo(original);
        }

        /**
         * Corrupt input must reach callers as {@link IOException}, never as the engine library's own runtime exception:
         * callers of the size-discovering shape have no sized output to sanity-check against.
         */
        @Test
        void corruptInputThrowsIOException() {
            MemorySegment garbage =
                    MemorySegment.ofArray(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
            assertThatThrownBy(() -> Codec.snappy().decompress(garbage)).isInstanceOf(IOException.class);
            assertThatThrownBy(() -> Codec.zstd().decompress(garbage)).isInstanceOf(IOException.class);
            assertThatThrownBy(() -> Codec.deflate().decompress(garbage)).isInstanceOf(IOException.class);
            assertThatThrownBy(() -> Codec.gzip().decompress(garbage)).isInstanceOf(IOException.class);
            assertThatThrownBy(() -> Codec.bzip2().decompress(garbage)).isInstanceOf(IOException.class);
            assertThatThrownBy(() -> Codec.xz().decompress(garbage)).isInstanceOf(IOException.class);
        }

        @Test
        void rawBlockCodecsRejectSizeDiscovery() {
            MemorySegment src = MemorySegment.ofArray(new byte[] {0x01});
            assertThatThrownBy(() -> Codec.lz4Raw().decompress(src)).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> Codec.lzo().decompress(src)).isInstanceOf(UnsupportedOperationException.class);
        }
    }

    private static byte[] brotliHelloWorld() {
        return new byte[] {
            (byte) 0x0f, (byte) 0x05, (byte) 0x80, (byte) 0x68, (byte) 0x65,
            (byte) 0x6c, (byte) 0x6c, (byte) 0x6f, (byte) 0x20, (byte) 0x77,
            (byte) 0x6f, (byte) 0x72, (byte) 0x6c, (byte) 0x64, (byte) 0x03
        };
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
