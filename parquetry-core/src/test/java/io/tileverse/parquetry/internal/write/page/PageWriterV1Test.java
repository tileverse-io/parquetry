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
package io.tileverse.parquetry.internal.write.page;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.data.WriteOptions.ParquetVersion;
import io.tileverse.parquetry.format.DataPageHeader;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.internal.read.page.LevelDecoder;
import io.tileverse.parquetry.internal.write.ColumnContext;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.testsupport.ByteArrayWritableChannel;

class PageWriterV1Test {

    @Test
    void v1DataPageRoundTrips() throws Exception {
        int[] values = {1, 2, 3, 4, 5};
        ColumnContext column = new ColumnContext(0, 0, PrimitiveKind.INT32, ParquetVersion.V1_1, Compression.zstd(3));
        PageWriter writer = new PageWriter(column);

        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        PageEncodeJob job = new PageEncodeJob(
                values,
                values.length,
                0,
                values.length,
                null,
                null,
                new PlainInt32Encoder(),
                pageStats(encodeInt32(1), encodeInt32(5), 0, false));
        EncodedPage encoded = writer.writeDataPageV1(job, out);

        byte[] wire = out.toByteArray();
        ByteArrayInputStream in = new ByteArrayInputStream(wire);
        PageHeader header = ParquetFormat.readPageHeader(in);

        assertThat(header.type()).isEqualTo(PageType.DATA_PAGE);
        DataPageHeader v1 = header.dataPageHeader().orElseThrow();
        assertThat(v1.numValues()).isEqualTo(values.length);
        assertThat(v1.encoding()).isEqualTo(Encoding.PLAIN);
        assertThat(v1.repetitionLevelEncoding()).isEqualTo(Encoding.RLE);
        assertThat(v1.definitionLevelEncoding()).isEqualTo(Encoding.RLE);

        Statistics st = v1.statistics().orElseThrow();
        assertThat(toBytes(st.minValue())).containsExactly(encodeInt32(1));
        assertThat(toBytes(st.maxValue())).containsExactly(encodeInt32(5));

        byte[] tail = remaining(in);
        Compression codec = Compression.zstd(3);
        byte[] decompressed = decompress(codec, tail, header.uncompressedPageSize());
        // Flat REQUIRED column: no level prefixes, only values.
        int[] decoded = decodeInt32sLittleEndian(decompressed, values.length);
        assertThat(decoded).containsExactly(values);
        assertThat(encoded.totalBytes()).isEqualTo(wire.length);
    }

    @Test
    void v1NestedColumnPrefixesLevelsInsideCompressedBlob() throws Exception {
        int[] values = {11, 22, 33};
        int[] repLevels = {0, 1, 1};
        int[] defLevels = {2, 2, 2};
        ColumnContext column =
                new ColumnContext(1, 2, PrimitiveKind.INT32, ParquetVersion.V1_1, Compression.uncompressed());
        PageWriter writer = new PageWriter(column);

        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        PageEncodeJob job = new PageEncodeJob(
                values,
                values.length,
                0,
                values.length,
                repLevels,
                defLevels,
                new PlainInt32Encoder(),
                pageStats(encodeInt32(11), encodeInt32(33), 0, false));
        writer.writeDataPageV1(job, out);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        PageHeader header = ParquetFormat.readPageHeader(in);
        assertThat(header.type()).isEqualTo(PageType.DATA_PAGE);
        byte[] payload = remaining(in);
        ByteBuffer cursor = ByteBuffer.wrap(payload).order(LITTLE_ENDIAN);

        int repLen = cursor.getInt();
        ByteBuffer repSlice = cursor.slice().limit(repLen).order(LITTLE_ENDIAN);
        cursor.position(cursor.position() + repLen);
        int defLen = cursor.getInt();
        ByteBuffer defSlice = cursor.slice().limit(defLen).order(LITTLE_ENDIAN);
        cursor.position(cursor.position() + defLen);

        int[] decodedRep = new int[repLevels.length];
        LevelDecoder repDecoder = new LevelDecoder(LevelDecoder.computeBitWidth(1));
        repDecoder.load(MemorySegment.ofBuffer(repSlice));
        repDecoder.decode(repLevels.length, decodedRep, 0);
        assertThat(decodedRep).containsExactly(repLevels);

        int[] decodedDef = new int[defLevels.length];
        LevelDecoder defDecoder = new LevelDecoder(LevelDecoder.computeBitWidth(2));
        defDecoder.load(MemorySegment.ofBuffer(defSlice));
        defDecoder.decode(defLevels.length, decodedDef, 0);
        assertThat(decodedDef).containsExactly(defLevels);

        byte[] valueBytes = new byte[cursor.remaining()];
        cursor.get(valueBytes);
        int[] decodedValues = decodeInt32sLittleEndian(valueBytes, values.length);
        assertThat(decodedValues).containsExactly(values);
    }

    @Test
    void v1UncompressedDoesNotCompress() throws Exception {
        int[] values = {99};
        ColumnContext column =
                new ColumnContext(0, 0, PrimitiveKind.INT32, ParquetVersion.V1_1, Compression.uncompressed());
        PageWriter writer = new PageWriter(column);

        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        PageEncodeJob job = new PageEncodeJob(
                values,
                values.length,
                0,
                values.length,
                null,
                null,
                new PlainInt32Encoder(),
                pageStats(encodeInt32(99), encodeInt32(99), 0, false));
        writer.writeDataPageV1(job, out);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        PageHeader header = ParquetFormat.readPageHeader(in);
        assertThat(header.compressedPageSize()).isEqualTo(header.uncompressedPageSize());

        byte[] tail = remaining(in);
        int[] decoded = decodeInt32sLittleEndian(tail, values.length);
        assertThat(decoded).containsExactly(values);
    }

    @Test
    void v1AllNullPageOmitsMinMax() throws Exception {
        int[] defLevels = {0, 0, 0};
        ColumnContext column = new ColumnContext(0, 1, PrimitiveKind.INT32, ParquetVersion.V1_1, Compression.zstd(3));
        PageWriter writer = new PageWriter(column);

        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        PageEncodeJob job = new PageEncodeJob(
                new int[0],
                defLevels.length,
                defLevels.length,
                defLevels.length,
                null,
                defLevels,
                new PlainInt32Encoder(),
                new PageStatistics(MemorySegment.NULL, MemorySegment.NULL, defLevels.length, true));
        writer.writeDataPageV1(job, out);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        PageHeader header = ParquetFormat.readPageHeader(in);
        DataPageHeader v1 = header.dataPageHeader().orElseThrow();
        Statistics st = v1.statistics().orElseThrow();
        assertThat(st.nullCount()).hasValue((long) defLevels.length);
        assertThat(st.minValue().byteSize()).isZero();
        assertThat(st.maxValue().byteSize()).isZero();
    }

    @Test
    void v1WithSnappyRoundTrips() throws Exception {
        int[] values = {-7, 0, 7, 13, 21};
        ColumnContext column = new ColumnContext(0, 0, PrimitiveKind.INT32, ParquetVersion.V1_1, Compression.snappy());
        PageWriter writer = new PageWriter(column);

        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        PageEncodeJob job = new PageEncodeJob(
                values,
                values.length,
                0,
                values.length,
                null,
                null,
                new PlainInt32Encoder(),
                pageStats(encodeInt32(-7), encodeInt32(21), 0, false));
        writer.writeDataPageV1(job, out);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        PageHeader header = ParquetFormat.readPageHeader(in);
        byte[] tail = remaining(in);
        Compression codec = Compression.snappy();
        byte[] decompressed = decompress(codec, tail, header.uncompressedPageSize());
        int[] decoded = decodeInt32sLittleEndian(decompressed, values.length);
        assertThat(decoded).containsExactly(values);
    }

    // --- helpers ---

    private static PageStatistics pageStats(byte[] min, byte[] max, long nullCount, boolean isNullPage) {
        return new PageStatistics(
                MemorySegment.ofArray(min).asReadOnly(),
                MemorySegment.ofArray(max).asReadOnly(),
                nullCount,
                isNullPage);
    }

    private static byte[] encodeInt32(int value) {
        byte[] out = new byte[Integer.BYTES];
        ByteBuffer.wrap(out).order(LITTLE_ENDIAN).putInt(value);
        return out;
    }

    private static byte[] toBytes(MemorySegment segment) {
        return segment.toArray(ValueLayout.JAVA_BYTE);
    }

    private static byte[] remaining(ByteArrayInputStream in) {
        byte[] out = new byte[in.available()];
        int read = in.read(out, 0, out.length);
        assertThat(read).isEqualTo(out.length);
        return out;
    }

    private static byte[] decompress(Compression codec, byte[] compressed, int uncompressedSize) throws Exception {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment src = arena.allocate(compressed.length);
            MemorySegment.copy(compressed, 0, src, ValueLayout.JAVA_BYTE, 0L, compressed.length);
            MemorySegment dst = arena.allocate(uncompressedSize);
            codec.decompress(src, dst);
            byte[] out = new byte[uncompressedSize];
            MemorySegment.copy(dst, ValueLayout.JAVA_BYTE, 0L, out, 0, uncompressedSize);
            return out;
        }
    }

    private static int[] decodeInt32sLittleEndian(byte[] bytes, int count) {
        ByteBuffer view = ByteBuffer.wrap(bytes).order(LITTLE_ENDIAN);
        int[] out = new int[count];
        for (int i = 0; i < count; i++) {
            out[i] = view.getInt();
        }
        return out;
    }
}
