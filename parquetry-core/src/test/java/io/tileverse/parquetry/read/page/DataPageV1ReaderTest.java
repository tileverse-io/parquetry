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
package io.tileverse.parquetry.read.page;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.ParquetFormatException;
import io.tileverse.parquetry.codec.Codec;
import io.tileverse.parquetry.codec.CodecRegistry;
import io.tileverse.parquetry.format.DataPageHeader;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.enums.CompressionCodec;
import io.tileverse.parquetry.format.enums.Encoding;
import io.tileverse.parquetry.format.enums.PageType;
import io.tileverse.parquetry.read.LevelMaximaResolver.LevelMaxima;

import io.airlift.compress.v3.snappy.SnappyCompressor;
import io.tileverse.io.ByteBufferPool;

/**
 * Unit tests for {@link DataPageV1Reader} that exercise the V1 layout (one compressed blob carrying optional rep-level
 * and def-level length-prefixed sections followed by the values). The {@code UNCOMPRESSED} codec is used so the
 * post-decompression byte layout is the in-memory layout, letting us assert exact slice equality. One test exercises a
 * real codec (SNAPPY) to confirm the decompress-then-slice flow works end to end.
 */
class DataPageV1ReaderTest {

    private final DataPageV1Reader reader = new DataPageV1Reader();
    private final ByteBufferPool pool = new ByteBufferPool();
    private final Codec uncompressed = CodecRegistry.lookup(CompressionCodec.UNCOMPRESSED);

    @Test
    void readsV1PageWithBothRepAndDefLevels() throws IOException {
        byte[] repLevels = {0x01, 0x02, 0x03};
        byte[] defLevels = {0x07, 0x08, 0x09, 0x0A};
        int[] expected = {1, 2, 3};
        byte[] valueBytes = encodeInt32sLittleEndian(expected);

        byte[] payload = buildV1Payload(Optional.of(repLevels), Optional.of(defLevels), valueBytes);
        PageHeader header = newV1Header(expected.length, payload.length, Encoding.PLAIN);

        try (DecodedPage page =
                reader.read(header, new LevelMaxima(1, 2), ByteBuffer.wrap(payload), uncompressed, pool)) {
            assertThat(page.valueCount()).isEqualTo(expected.length);
            assertThat(toBytes(page.repLevelBytes())).containsExactly(repLevels);
            assertThat(toBytes(page.defLevelBytes())).containsExactly(defLevels);
            assertThat(page.valuesEncoding()).isEqualTo(Encoding.PLAIN);

            int[] decoded = decodeInt32sLittleEndian(page.values(), expected.length);
            assertThat(decoded).containsExactly(expected);
        }
        assertThat(outstandingBorrows(pool)).isZero();
    }

    @Test
    void readsV1PageWithNoLevelsWhenColumnHasZeroMaxima() throws IOException {
        int[] expected = {10, 20, 30, 40};
        byte[] valueBytes = encodeInt32sLittleEndian(expected);

        byte[] payload = valueBytes;
        PageHeader header = newV1Header(expected.length, payload.length, Encoding.PLAIN);

        try (DecodedPage page =
                reader.read(header, new LevelMaxima(0, 0), ByteBuffer.wrap(payload), uncompressed, pool)) {
            assertThat(page.repLevelBytes().remaining())
                    .as("rep slice is empty when maxRep=0")
                    .isZero();
            assertThat(page.defLevelBytes().remaining())
                    .as("def slice is empty when maxDef=0")
                    .isZero();
            int[] decoded = decodeInt32sLittleEndian(page.values(), expected.length);
            assertThat(decoded).containsExactly(expected);
        }
        assertThat(outstandingBorrows(pool)).isZero();
    }

    @Test
    void readsV1PageWithOnlyDefLevels() throws IOException {
        byte[] defLevels = {0x11, 0x12, 0x13};
        int[] expected = {7, 8};
        byte[] valueBytes = encodeInt32sLittleEndian(expected);

        byte[] payload = buildV1Payload(Optional.empty(), Optional.of(defLevels), valueBytes);
        PageHeader header = newV1Header(expected.length, payload.length, Encoding.PLAIN);

        try (DecodedPage page =
                reader.read(header, new LevelMaxima(0, 1), ByteBuffer.wrap(payload), uncompressed, pool)) {
            assertThat(page.repLevelBytes().remaining())
                    .as("rep slice is empty when maxRep=0")
                    .isZero();
            assertThat(toBytes(page.defLevelBytes())).containsExactly(defLevels);

            int[] decoded = decodeInt32sLittleEndian(page.values(), expected.length);
            assertThat(decoded).containsExactly(expected);
        }
        assertThat(outstandingBorrows(pool)).isZero();
    }

    @Test
    void normalizesPlainDictionaryToRleDictionary() throws IOException {
        int[] expected = {5, 6};
        byte[] valueBytes = encodeInt32sLittleEndian(expected);
        byte[] payload = valueBytes;
        PageHeader header = newV1Header(expected.length, payload.length, Encoding.PLAIN_DICTIONARY);

        try (DecodedPage page =
                reader.read(header, new LevelMaxima(0, 0), ByteBuffer.wrap(payload), uncompressed, pool)) {
            assertThat(page.valuesEncoding()).isEqualTo(Encoding.RLE_DICTIONARY);
        }
        assertThat(outstandingBorrows(pool)).isZero();
    }

    @Test
    void exercisesCodecWithSnappy() throws IOException {
        Codec snappy = CodecRegistry.lookup(CompressionCodec.SNAPPY);
        byte[] repLevels = {0x21, 0x22};
        byte[] defLevels = {0x33, 0x34, 0x35};
        int[] expected = {100, 200, 300, 400};
        byte[] valueBytes = encodeInt32sLittleEndian(expected);
        byte[] uncompressedPayload = buildV1Payload(Optional.of(repLevels), Optional.of(defLevels), valueBytes);

        byte[] compressedPayload = snappyCompress(uncompressedPayload);
        PageHeader header = new PageHeader(
                PageType.DATA_PAGE,
                /*uncompressedPageSize*/ uncompressedPayload.length,
                /*compressedPageSize*/ compressedPayload.length,
                /*crc*/ OptionalInt.empty(),
                Optional.of(new DataPageHeader(
                        expected.length, Encoding.PLAIN, Encoding.RLE, Encoding.RLE, Optional.empty())),
                /*indexPageHeader*/ Optional.empty(),
                /*dictionaryPageHeader*/ Optional.empty(),
                /*dataPageHeaderV2*/ Optional.empty());

        try (DecodedPage page =
                reader.read(header, new LevelMaxima(1, 2), ByteBuffer.wrap(compressedPayload), snappy, pool)) {
            assertThat(toBytes(page.repLevelBytes())).containsExactly(repLevels);
            assertThat(toBytes(page.defLevelBytes())).containsExactly(defLevels);
            int[] decoded = decodeInt32sLittleEndian(page.values(), expected.length);
            assertThat(decoded).containsExactly(expected);
        }
        assertThat(outstandingBorrows(pool)).isZero();
    }

    @Test
    void rejectsNegativeLevelLengthPrefix() {
        byte[] badRepLenPrefix = leInt(-1);
        byte[] payload = concat(badRepLenPrefix, new byte[] {0x00, 0x00, 0x00, 0x00});
        PageHeader header = newV1Header(0, payload.length, Encoding.PLAIN);
        ByteBuffer wrapped = ByteBuffer.wrap(payload);
        LevelMaxima levels = new LevelMaxima(1, 0);

        assertThatThrownBy(() -> reader.read(header, levels, wrapped, uncompressed, pool))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("negative");
        assertThat(outstandingBorrows(pool))
                .as("borrowed buffer must be returned on failure")
                .isZero();
    }

    @Test
    void rejectsLevelLengthExceedingRemainingBytes() {
        int payloadBytes = 8;
        byte[] payload = new byte[payloadBytes];
        ByteBuffer.wrap(payload).order(ByteOrder.LITTLE_ENDIAN).putInt(payloadBytes); // claim too many rep bytes
        PageHeader header = newV1Header(0, payload.length, Encoding.PLAIN);
        ByteBuffer wrapped = ByteBuffer.wrap(payload);
        LevelMaxima levels = new LevelMaxima(1, 0);

        assertThatThrownBy(() -> reader.read(header, levels, wrapped, uncompressed, pool))
                .isInstanceOf(ParquetFormatException.class);
        assertThat(outstandingBorrows(pool)).isZero();
    }

    @Test
    void doubleCloseIsIdempotent() throws IOException {
        byte[] payload = encodeInt32sLittleEndian(new int[] {1});
        PageHeader header = newV1Header(1, payload.length, Encoding.PLAIN);
        DecodedPage page = reader.read(header, new LevelMaxima(0, 0), ByteBuffer.wrap(payload), uncompressed, pool);
        page.close();
        page.close();
        assertThat(outstandingBorrows(pool)).isZero();
    }

    // --- fixture builders ---

    private static PageHeader newV1Header(int numValues, int payloadLen, Encoding valuesEncoding) {
        return new PageHeader(
                PageType.DATA_PAGE,
                /*uncompressedPageSize*/ payloadLen,
                /*compressedPageSize*/ payloadLen,
                /*crc*/ OptionalInt.empty(),
                Optional.of(
                        new DataPageHeader(numValues, valuesEncoding, Encoding.RLE, Encoding.RLE, Optional.empty())),
                /*indexPageHeader*/ Optional.empty(),
                /*dictionaryPageHeader*/ Optional.empty(),
                /*dataPageHeaderV2*/ Optional.empty());
    }

    private static byte[] buildV1Payload(Optional<byte[]> repLevels, Optional<byte[]> defLevels, byte[] values) {
        int repPrefix = repLevels.map(b -> 4 + b.length).orElse(0);
        int defPrefix = defLevels.map(b -> 4 + b.length).orElse(0);
        ByteBuffer buf =
                ByteBuffer.allocate(repPrefix + defPrefix + values.length).order(ByteOrder.LITTLE_ENDIAN);
        repLevels.ifPresent(bytes -> {
            buf.putInt(bytes.length);
            buf.put(bytes);
        });
        defLevels.ifPresent(bytes -> {
            buf.putInt(bytes.length);
            buf.put(bytes);
        });
        buf.put(values);
        return buf.array();
    }

    private static byte[] snappyCompress(byte[] uncompressedBytes) {
        SnappyCompressor compressor = SnappyCompressor.create();
        byte[] output = new byte[compressor.maxCompressedLength(uncompressedBytes.length)];
        int written = compressor.compress(uncompressedBytes, 0, uncompressedBytes.length, output, 0, output.length);
        byte[] trimmed = new byte[written];
        System.arraycopy(output, 0, trimmed, 0, written);
        return trimmed;
    }

    private static byte[] leInt(int value) {
        return ByteBuffer.allocate(4)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(value)
                .array();
    }

    private static byte[] encodeInt32sLittleEndian(int[] values) {
        ByteBuffer buf = ByteBuffer.allocate(values.length * 4).order(ByteOrder.LITTLE_ENDIAN);
        for (int v : values) {
            buf.putInt(v);
        }
        return buf.array();
    }

    private static int[] decodeInt32sLittleEndian(ByteBuffer buf, int count) {
        ByteBuffer view = buf.duplicate().order(ByteOrder.LITTLE_ENDIAN);
        int[] out = new int[count];
        for (int i = 0; i < count; i++) {
            out[i] = view.getInt();
        }
        return out;
    }

    private static byte[] concat(byte[]... arrays) {
        int total = 0;
        for (byte[] a : arrays) {
            total += a.length;
        }
        byte[] out = new byte[total];
        int off = 0;
        for (byte[] a : arrays) {
            System.arraycopy(a, 0, out, off, a.length);
            off += a.length;
        }
        return out;
    }

    private static byte[] toBytes(ByteBuffer buf) {
        ByteBuffer dup = buf.duplicate();
        byte[] bytes = new byte[dup.remaining()];
        dup.get(bytes);
        return bytes;
    }

    private static long outstandingBorrows(ByteBufferPool pool) {
        ByteBufferPool.PoolStatistics stats = pool.getStatistics();
        return (stats.created() + stats.reused()) - (stats.returned() + stats.discarded());
    }
}
