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
package io.tileverse.parquetry.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.zip.Deflater;

import org.junit.jupiter.api.Test;

class BlockCodecTest {

    private static MemorySegment heap(byte[] bytes) {
        return MemorySegment.ofArray(bytes).asReadOnly();
    }

    private static byte[] rawDeflate(byte[] data) {
        Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, /*nowrap=*/ true);
        deflater.setInput(data);
        deflater.finish();
        byte[] buffer = new byte[data.length + 64];
        int written = deflater.deflate(buffer);
        deflater.end();
        byte[] out = new byte[written];
        System.arraycopy(buffer, 0, out, 0, written);
        return out;
    }

    @Test
    void nullCodecReturnsInputUnchanged() {
        MemorySegment input = heap(new byte[] {1, 2, 3});
        assertThat(BlockCodec.NULL.decompress(input)).isSameAs(input);
    }

    @Test
    void deflateRoundTripsLargePayload() {
        byte[] original = new byte[64 * 1024];
        new Random(42).nextBytes(original);
        for (int i = 0; i < 8192; i++) {
            original[i] = (byte) (i % 7);
        }
        MemorySegment compressed = heap(rawDeflate(original));
        MemorySegment decompressed = BlockCodec.DEFLATE.decompress(compressed);
        assertThat(decompressed.toArray(ValueLayout.JAVA_BYTE)).isEqualTo(original);
    }

    @Test
    void deflateWorksOnDirectSegments() {
        byte[] original = "the quick brown fox".repeat(100).getBytes();
        byte[] deflated = rawDeflate(original);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment compressed = arena.allocate(deflated.length);
            MemorySegment.copy(MemorySegment.ofArray(deflated), 0, compressed, 0, deflated.length);
            MemorySegment decompressed = BlockCodec.DEFLATE.decompress(compressed);
            assertThat(decompressed.toArray(ValueLayout.JAVA_BYTE)).isEqualTo(original);
        }
    }

    @Test
    void deflateRoundTripsEmptyPayload() {
        MemorySegment out = BlockCodec.DEFLATE.decompress(heap(rawDeflate(new byte[0])));
        assertThat(out.byteSize()).isZero();
    }

    @Test
    void deflateRoundTripsHighlyCompressiblePayload() {
        byte[] original = new byte[1 << 20];
        Arrays.fill(original, (byte) 7);
        MemorySegment compressed = heap(rawDeflate(original));
        MemorySegment decompressed = BlockCodec.DEFLATE.decompress(compressed);
        assertThat(decompressed.toArray(ValueLayout.JAVA_BYTE)).isEqualTo(original);
    }

    @Test
    void rejectsTruncatedDeflate() {
        byte[] full = rawDeflate("the quick brown fox".repeat(100).getBytes());
        int half = Math.max(1, full.length / 2);
        byte[] truncated = Arrays.copyOf(full, half);
        MemorySegment compressed = heap(truncated);
        assertThatThrownBy(() -> BlockCodec.DEFLATE.decompress(compressed)).isInstanceOf(AvroFormatException.class);
    }

    @Test
    void rejectsCorruptDeflate() {
        MemorySegment corrupt = heap(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
        assertThatThrownBy(() -> BlockCodec.DEFLATE.decompress(corrupt)).isInstanceOf(AvroFormatException.class);
    }

    @Test
    void rejectsUnsupportedCodecNames() {
        assertThatThrownBy(() -> BlockCodec.fromName("lzo")).isInstanceOf(AvroFormatException.class);
        assertThatThrownBy(() -> BlockCodec.fromName("nope")).isInstanceOf(AvroFormatException.class);
    }

    @Test
    void mapsCodecNames() {
        assertThat(BlockCodec.fromName(null)).isEqualTo(BlockCodec.NULL);
        assertThat(BlockCodec.fromName("null")).isEqualTo(BlockCodec.NULL);
        assertThat(BlockCodec.fromName("deflate")).isEqualTo(BlockCodec.DEFLATE);
        assertThat(BlockCodec.fromName("snappy")).isEqualTo(BlockCodec.SNAPPY);
        assertThat(BlockCodec.fromName("zstandard")).isEqualTo(BlockCodec.ZSTANDARD);
        assertThat(BlockCodec.fromName("bzip2")).isEqualTo(BlockCodec.BZIP2);
        assertThat(BlockCodec.fromName("xz")).isEqualTo(BlockCodec.XZ);
    }

    /** Builds the Avro snappy block framing with xerial's compressor: raw snappy block + big-endian CRC32. */
    private static byte[] xerialSnappyBlock(byte[] data) throws IOException {
        byte[] compressed = org.xerial.snappy.Snappy.compress(data);
        CRC32 crc = new CRC32();
        crc.update(data);
        ByteBuffer out = ByteBuffer.allocate(compressed.length + Integer.BYTES);
        out.put(compressed);
        out.putInt((int) crc.getValue());
        return out.array();
    }

    @Test
    void snappyDecodesXerialWrittenBlocks() throws IOException {
        byte[] original = "snappy framing oracle payload ".repeat(64).getBytes();
        MemorySegment decompressed = BlockCodec.SNAPPY.decompress(heap(xerialSnappyBlock(original)));
        assertThat(decompressed.toArray(ValueLayout.JAVA_BYTE)).isEqualTo(original);
    }

    @Test
    void snappyRejectsCrcMismatch() throws IOException {
        byte[] original = "snappy payload".getBytes();
        byte[] block = xerialSnappyBlock(original);
        block[block.length - 1] ^= 0x01;
        MemorySegment corrupted = heap(block);
        assertThatThrownBy(() -> BlockCodec.SNAPPY.decompress(corrupted))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("CRC32");
    }

    @Test
    void snappyRejectsBlockShorterThanCrc() {
        MemorySegment tooShort = heap(new byte[] {1, 2, 3});
        assertThatThrownBy(() -> BlockCodec.SNAPPY.decompress(tooShort)).isInstanceOf(AvroFormatException.class);
    }

    @Test
    void zstandardDecodesLubenWrittenFrames() {
        byte[] original = "zstandard framing oracle payload ".repeat(64).getBytes();
        byte[] compressed = com.github.luben.zstd.Zstd.compress(original);
        MemorySegment decompressed = BlockCodec.ZSTANDARD.decompress(heap(compressed));
        assertThat(decompressed.toArray(ValueLayout.JAVA_BYTE)).isEqualTo(original);
    }

    @Test
    void zstandardRejectsCorruptFrame() {
        MemorySegment corrupt = heap(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
        assertThatThrownBy(() -> BlockCodec.ZSTANDARD.decompress(corrupt)).isInstanceOf(AvroFormatException.class);
    }

    @Test
    void bzip2RejectsCorruptStream() {
        MemorySegment corrupt = heap(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
        assertThatThrownBy(() -> BlockCodec.BZIP2.decompress(corrupt)).isInstanceOf(AvroFormatException.class);
    }

    @Test
    void xzRejectsCorruptStream() {
        MemorySegment corrupt = heap(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
        assertThatThrownBy(() -> BlockCodec.XZ.decompress(corrupt)).isInstanceOf(AvroFormatException.class);
    }
}
