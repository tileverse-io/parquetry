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
import io.tileverse.parquetry.format.DataPageHeaderV2;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.internal.write.ColumnContext;
import io.tileverse.parquetry.schema.PrimitiveKind;

class PageWriterPreEncodedTest {

    @Test
    void v2PreEncodedRoundTripsWithPlainInt32Bytes() throws Exception {
        int[] values = {10, 20, 30, 40};
        byte[] plainBytes = encodeInt32sLittleEndian(values);
        MemorySegment encoded = MemorySegment.ofArray(plainBytes).asReadOnly();

        ColumnContext column = new ColumnContext(0, 0, PrimitiveKind.INT32, ParquetVersion.V2_0, Compression.zstd(3));
        PageWriter writer = new PageWriter(column);

        GrowableByteSink out = new GrowableByteSink(64);
        PreEncodedPageJob job =
                new PreEncodedPageJob(encoded, Encoding.PLAIN, values.length, 0, values.length, null, null, null);
        EncodedPage result = writer.writeDataPageV2PreEncoded(job, out);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        PageHeader header = ParquetFormat.readPageHeader(in);
        assertThat(header.type()).isEqualTo(PageType.DATA_PAGE_V2);
        DataPageHeaderV2 v2 = header.dataPageHeaderV2().orElseThrow();
        assertThat(v2.encoding()).isEqualTo(Encoding.PLAIN);
        assertThat(v2.numValues()).isEqualTo(values.length);
        assertThat(v2.numNulls()).isZero();
        assertThat(v2.repetitionLevelsByteLength()).isZero();
        assertThat(v2.definitionLevelsByteLength()).isZero();

        byte[] tail = remaining(in);
        Compression codec = Compression.zstd(3);
        byte[] decompressed = decompress(codec, tail, header.uncompressedPageSize());
        int[] decoded = decodeInt32sLittleEndian(decompressed, values.length);
        assertThat(decoded).containsExactly(values);

        assertThat(result.payloadBytes()).isEqualTo(header.compressedPageSize());
        assertThat(result.totalBytes()).isEqualTo(out.size());
    }

    @Test
    void v2PreEncodedPreservesRleDictionaryMarker() throws Exception {
        int[] dictIndices = {0, 1, 0, 2};
        GrowableByteSink dictPayload = new GrowableByteSink(64);
        new RleDictionaryEncoder().encode(dictIndices, dictIndices.length, dictPayload);
        byte[] dictBytes = dictPayload.toByteArray();
        MemorySegment encoded = MemorySegment.ofArray(dictBytes).asReadOnly();

        ColumnContext column = new ColumnContext(0, 0, PrimitiveKind.INT32, ParquetVersion.V2_0, Compression.zstd(3));
        PageWriter writer = new PageWriter(column);

        GrowableByteSink out = new GrowableByteSink(64);
        PreEncodedPageJob job = new PreEncodedPageJob(
                encoded, Encoding.RLE_DICTIONARY, dictIndices.length, 0, dictIndices.length, null, null, null);
        writer.writeDataPageV2PreEncoded(job, out);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        PageHeader header = ParquetFormat.readPageHeader(in);
        DataPageHeaderV2 v2 = header.dataPageHeaderV2().orElseThrow();
        assertThat(v2.encoding()).isEqualTo(Encoding.RLE_DICTIONARY);
        assertThat(v2.numValues()).isEqualTo(dictIndices.length);

        byte[] tail = remaining(in);
        Compression codec = Compression.zstd(3);
        byte[] decompressed = decompress(codec, tail, header.uncompressedPageSize());
        assertThat(decompressed).containsExactly(dictBytes);
    }

    @Test
    void v1PreEncodedRoundTripsWithRepDefLevels() throws Exception {
        int[] values = {5, 9};
        int[] repLevels = {0, 1};
        int[] defLevels = {1, 1};
        byte[] plainBytes = encodeInt32sLittleEndian(values);
        MemorySegment encoded = MemorySegment.ofArray(plainBytes).asReadOnly();

        ColumnContext column =
                new ColumnContext(1, 1, PrimitiveKind.INT32, ParquetVersion.V1_1, Compression.uncompressed());
        PageWriter writer = new PageWriter(column);

        GrowableByteSink out = new GrowableByteSink(64);
        PreEncodedPageJob job =
                new PreEncodedPageJob(encoded, Encoding.PLAIN, values.length, 0, 0, repLevels, defLevels, null);
        writer.writeDataPageV1PreEncoded(job, out);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        PageHeader header = ParquetFormat.readPageHeader(in);
        assertThat(header.type()).isEqualTo(PageType.DATA_PAGE);
        DataPageHeader v1 = header.dataPageHeader().orElseThrow();
        assertThat(v1.numValues()).isEqualTo(values.length);
        assertThat(v1.encoding()).isEqualTo(Encoding.PLAIN);
        assertThat(v1.definitionLevelEncoding()).isEqualTo(Encoding.RLE);
        assertThat(v1.repetitionLevelEncoding()).isEqualTo(Encoding.RLE);

        byte[] payload = remaining(in);
        ByteBuffer view = ByteBuffer.wrap(payload).order(LITTLE_ENDIAN);
        int repBytes = view.getInt();
        assertThat(repBytes).isPositive();
        view.position(view.position() + repBytes);
        int defBytes = view.getInt();
        assertThat(defBytes).isPositive();
        view.position(view.position() + defBytes);

        int[] decoded = new int[values.length];
        for (int i = 0; i < values.length; i++) {
            decoded[i] = view.getInt();
        }
        assertThat(decoded).containsExactly(values);
    }

    private static byte[] encodeInt32sLittleEndian(int[] values) {
        byte[] out = new byte[values.length * Integer.BYTES];
        ByteBuffer view = ByteBuffer.wrap(out).order(LITTLE_ENDIAN);
        for (int value : values) {
            view.putInt(value);
        }
        return out;
    }

    private static int[] decodeInt32sLittleEndian(byte[] bytes, int count) {
        ByteBuffer view = ByteBuffer.wrap(bytes).order(LITTLE_ENDIAN);
        int[] out = new int[count];
        for (int i = 0; i < count; i++) {
            out[i] = view.getInt();
        }
        return out;
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
}
