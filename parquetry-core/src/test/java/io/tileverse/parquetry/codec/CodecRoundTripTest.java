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
package io.tileverse.parquetry.codec;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.CompressionCodec;

import io.airlift.compress.v3.lz4.Lz4Compressor;
import io.airlift.compress.v3.snappy.SnappyCompressor;
import io.airlift.compress.v3.zstd.ZstdCompressor;

/**
 * Round-trip checks for each {@link Codec} against a representative set of byte payloads: short edge cases, medium and
 * large buffers, all-zero, all-0xFF, alternating bits, ASCII text, and a fixed-seed pseudo-random buffer.
 */
class CodecRoundTripTest {

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

        Codec codec = CodecRegistry.lookup(CompressionCodec.SNAPPY);
        ByteBuffer out = codec.decompress(ByteBuffer.wrap(compressed, 0, compressedLen), original.length);
        assertRemainingEquals(out, original);
    }

    @ParameterizedTest(name = "zstd/{0}")
    @MethodSource("payloads")
    void zstdRoundTrip(String name, byte[] original) throws Exception {
        ZstdCompressor compressor = ZstdCompressor.create();
        byte[] compressed = new byte[compressor.maxCompressedLength(original.length)];
        int compressedLen = compressor.compress(original, 0, original.length, compressed, 0, compressed.length);

        Codec codec = CodecRegistry.lookup(CompressionCodec.ZSTD);
        ByteBuffer out = codec.decompress(ByteBuffer.wrap(compressed, 0, compressedLen), original.length);
        assertRemainingEquals(out, original);
    }

    @ParameterizedTest(name = "lz4raw/{0}")
    @MethodSource("payloads")
    void lz4RawRoundTrip(String name, byte[] original) throws Exception {
        Lz4Compressor compressor = Lz4Compressor.create();
        byte[] compressed = new byte[compressor.maxCompressedLength(original.length)];
        int compressedLen = compressor.compress(original, 0, original.length, compressed, 0, compressed.length);

        Codec codec = CodecRegistry.lookup(CompressionCodec.LZ4_RAW);
        ByteBuffer out = codec.decompress(ByteBuffer.wrap(compressed, 0, compressedLen), original.length);
        assertRemainingEquals(out, original);
    }

    @ParameterizedTest(name = "gzip/{0}")
    @MethodSource("payloads")
    void gzipRoundTrip(String name, byte[] original) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (GZIPOutputStream gz = new GZIPOutputStream(bos)) {
            gz.write(original);
        }
        byte[] compressed = bos.toByteArray();

        Codec codec = CodecRegistry.lookup(CompressionCodec.GZIP);
        ByteBuffer out = codec.decompress(ByteBuffer.wrap(compressed), original.length);
        assertRemainingEquals(out, original);
    }

    /**
     * Brotli round-trip using a hardcoded fixture. The org.brotli:dec library is decoder-only; there is no encoder
     * available on the classpath. The fixture was produced by: {@code echo -n "hello world" | brotli --stdout | xxd -i}
     */
    @Test
    void brotliFixture() throws Exception {
        byte[] compressed = {
            (byte) 0x0f, (byte) 0x05, (byte) 0x80, (byte) 0x68, (byte) 0x65,
            (byte) 0x6c, (byte) 0x6c, (byte) 0x6f, (byte) 0x20, (byte) 0x77,
            (byte) 0x6f, (byte) 0x72, (byte) 0x6c, (byte) 0x64, (byte) 0x03
        };
        byte[] expected = "hello world".getBytes();

        Codec codec = CodecRegistry.lookup(CompressionCodec.BROTLI);
        ByteBuffer out = codec.decompress(ByteBuffer.wrap(compressed), expected.length);
        assertRemainingEquals(out, expected);
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

    private static void assertRemainingEquals(ByteBuffer out, byte[] expected) {
        byte[] decoded = new byte[out.remaining()];
        out.get(decoded);
        assertThat(decoded).isEqualTo(expected);
    }
}
