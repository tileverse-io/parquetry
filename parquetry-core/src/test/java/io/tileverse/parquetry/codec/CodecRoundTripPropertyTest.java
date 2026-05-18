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
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.enums.CompressionCodec;

import io.airlift.compress.v3.lz4.Lz4Compressor;
import io.airlift.compress.v3.snappy.SnappyCompressor;
import io.airlift.compress.v3.zstd.ZstdCompressor;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.Size;

class CodecRoundTripPropertyTest {

    @Property
    void snappyRoundTrip(@ForAll @Size(min = 1, max = 4096) byte[] original) throws Exception {
        SnappyCompressor compressor = SnappyCompressor.create();
        byte[] compressed = new byte[compressor.maxCompressedLength(original.length)];
        int compressedLen = compressor.compress(original, 0, original.length, compressed, 0, compressed.length);

        Codec codec = CodecRegistry.lookup(CompressionCodec.SNAPPY);
        ByteBuffer out = codec.decompress(ByteBuffer.wrap(compressed, 0, compressedLen), original.length);
        byte[] decoded = new byte[out.remaining()];
        out.get(decoded);
        assertThat(decoded).isEqualTo(original);
    }

    @Property
    void zstdRoundTrip(@ForAll @Size(min = 1, max = 4096) byte[] original) throws Exception {
        ZstdCompressor compressor = ZstdCompressor.create();
        byte[] compressed = new byte[compressor.maxCompressedLength(original.length)];
        int compressedLen = compressor.compress(original, 0, original.length, compressed, 0, compressed.length);

        Codec codec = CodecRegistry.lookup(CompressionCodec.ZSTD);
        ByteBuffer out = codec.decompress(ByteBuffer.wrap(compressed, 0, compressedLen), original.length);
        byte[] decoded = new byte[out.remaining()];
        out.get(decoded);
        assertThat(decoded).isEqualTo(original);
    }

    @Property
    void lz4RawRoundTrip(@ForAll @Size(min = 1, max = 4096) byte[] original) throws Exception {
        Lz4Compressor compressor = Lz4Compressor.create();
        byte[] compressed = new byte[compressor.maxCompressedLength(original.length)];
        int compressedLen = compressor.compress(original, 0, original.length, compressed, 0, compressed.length);

        Codec codec = CodecRegistry.lookup(CompressionCodec.LZ4_RAW);
        ByteBuffer out = codec.decompress(ByteBuffer.wrap(compressed, 0, compressedLen), original.length);
        byte[] decoded = new byte[out.remaining()];
        out.get(decoded);
        assertThat(decoded).isEqualTo(original);
    }

    @Property
    void gzipRoundTrip(@ForAll @Size(min = 1, max = 4096) byte[] original) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (GZIPOutputStream gz = new GZIPOutputStream(bos)) {
            gz.write(original);
        }
        byte[] compressed = bos.toByteArray();

        Codec codec = CodecRegistry.lookup(CompressionCodec.GZIP);
        ByteBuffer out = codec.decompress(ByteBuffer.wrap(compressed), original.length);
        byte[] decoded = new byte[out.remaining()];
        out.get(decoded);
        assertThat(decoded).isEqualTo(original);
    }

    /**
     * Brotli round-trip using a hardcoded fixture. The org.brotli:dec library is decoder-only;
     * there is no encoder available on the classpath. The fixture was produced by:
     * {@code echo -n "hello world" | brotli --stdout | xxd -i}
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
        byte[] decoded = new byte[out.remaining()];
        out.get(decoded);
        assertThat(decoded).isEqualTo(expected);
    }
}
