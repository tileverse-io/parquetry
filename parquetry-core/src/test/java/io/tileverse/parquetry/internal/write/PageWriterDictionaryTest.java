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
package io.tileverse.parquetry.internal.write;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.data.WriteOptions.ParquetVersion;
import io.tileverse.parquetry.format.DictionaryPageHeader;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.internal.read.page.Dictionary;
import io.tileverse.parquetry.internal.read.page.DictionaryDecoder;
import io.tileverse.parquetry.internal.write.page.EncodedPage;
import io.tileverse.parquetry.internal.write.page.PageWriter;
import io.tileverse.parquetry.internal.write.page.PlainInt32Encoder;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.testsupport.ByteArrayWritableChannel;

class PageWriterDictionaryTest {

    @Test
    void dictionaryPageRoundTripsForV2Column() throws Exception {
        int[] dictionaryValues = {7, 11, 13, 17, 19};
        ColumnContext column = new ColumnContext(0, 0, PrimitiveKind.INT32, ParquetVersion.V2_0, Compression.zstd(3));
        PageWriter writer = new PageWriter(column);

        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        EncodedPage encoded =
                writer.writeDictionaryPage(dictionaryValues, dictionaryValues.length, new PlainInt32Encoder(), out);

        byte[] wire = out.toByteArray();
        ByteArrayInputStream in = new ByteArrayInputStream(wire);
        PageHeader header = ParquetFormat.readPageHeader(in);

        assertThat(header.type()).isEqualTo(PageType.DICTIONARY_PAGE);
        DictionaryPageHeader dictHeader = header.dictionaryPageHeader().orElseThrow();
        assertThat(dictHeader.numValues()).isEqualTo(dictionaryValues.length);
        assertThat(dictHeader.encoding())
                .as("V2 columns mark the dictionary page encoding as PLAIN")
                .isEqualTo(Encoding.PLAIN);
        assertThat(dictHeader.isSorted()).isFalse();

        byte[] compressedPayload = remaining(in);
        Compression codec = Compression.zstd(3);
        byte[] decompressed = decompress(codec, compressedPayload, header.uncompressedPageSize());

        Dictionary<?> dictionary = DictionaryDecoder.read(
                MemorySegment.ofBuffer(ByteBuffer.wrap(decompressed).order(LITTLE_ENDIAN)),
                PrimitiveKind.INT32,
                dictionaryValues.length,
                OptionalInt.empty());

        assertThat(dictionary).isInstanceOf(Dictionary.IntDict.class);
        Dictionary.IntDict intDict = (Dictionary.IntDict) dictionary;
        assertThat(intDict.size()).isEqualTo(dictionaryValues.length);
        int[] decoded = new int[dictionaryValues.length];
        for (int i = 0; i < dictionaryValues.length; i++) {
            decoded[i] = intDict.get(i);
        }
        assertThat(decoded).containsExactly(dictionaryValues);
        assertThat(encoded.totalBytes()).isEqualTo(wire.length);
    }

    @Test
    void dictionaryPageEncodingIsPlainDictionaryForV1Column() throws Exception {
        int[] dictionaryValues = {1, 2, 3};
        ColumnContext column =
                new ColumnContext(0, 0, PrimitiveKind.INT32, ParquetVersion.V1_1, Compression.uncompressed());
        PageWriter writer = new PageWriter(column);

        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        writer.writeDictionaryPage(dictionaryValues, dictionaryValues.length, new PlainInt32Encoder(), out);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        PageHeader header = ParquetFormat.readPageHeader(in);
        DictionaryPageHeader dictHeader = header.dictionaryPageHeader().orElseThrow();

        assertThat(dictHeader.encoding()).isEqualTo(Encoding.PLAIN_DICTIONARY);
        assertThat(header.compressedPageSize()).isEqualTo(header.uncompressedPageSize());
    }

    @Test
    void dictionaryPageWithSnappyRoundTrips() throws Exception {
        int[] dictionaryValues = {100, 200, 300, 400, 500, 600};
        ColumnContext column = new ColumnContext(0, 0, PrimitiveKind.INT32, ParquetVersion.V2_0, Compression.snappy());
        PageWriter writer = new PageWriter(column);

        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        writer.writeDictionaryPage(dictionaryValues, dictionaryValues.length, new PlainInt32Encoder(), out);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        PageHeader header = ParquetFormat.readPageHeader(in);
        byte[] compressedPayload = remaining(in);
        Compression codec = Compression.snappy();
        byte[] decompressed = decompress(codec, compressedPayload, header.uncompressedPageSize());

        Dictionary.IntDict dict = (Dictionary.IntDict) DictionaryDecoder.read(
                MemorySegment.ofBuffer(ByteBuffer.wrap(decompressed).order(LITTLE_ENDIAN)),
                PrimitiveKind.INT32,
                dictionaryValues.length,
                OptionalInt.empty());

        int[] decoded = new int[dictionaryValues.length];
        for (int i = 0; i < dictionaryValues.length; i++) {
            decoded[i] = dict.get(i);
        }
        assertThat(decoded).containsExactly(dictionaryValues);
    }

    // --- helpers ---

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
