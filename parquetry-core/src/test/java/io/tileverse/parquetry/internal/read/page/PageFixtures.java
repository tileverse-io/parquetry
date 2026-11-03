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
package io.tileverse.parquetry.internal.read.page;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import io.tileverse.parquetry.format.DataPageHeader;
import io.tileverse.parquetry.format.DataPageHeaderV2;
import io.tileverse.parquetry.format.DictionaryPageHeader;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.Statistics;

/**
 * Builds column-chunk bytes for a {@link PageCursor} walk or a {@link PageHeaderScan} directly, without writing a
 * Parquet file.
 */
final class PageFixtures {

    private PageFixtures() {}

    /**
     * The bytes of one column chunk over a flat required INT64 column: one uncompressed V1 data page per entry of
     * {@code valueCounts}, each page holding that many plain-encoded values. A flat required column has no repetition
     * or definition levels, hence each page payload is its value bytes alone.
     */
    static MemorySegment v1FlatChunk(int... valueCounts) {
        ByteArrayOutputStream chunk = new ByteArrayOutputStream();
        for (int valueCount : valueCounts) {
            appendV1FlatPage(chunk, valueCount);
        }
        return MemorySegment.ofArray(chunk.toByteArray()).asReadOnly();
    }

    private static void appendV1FlatPage(ByteArrayOutputStream chunk, int valueCount) {
        byte[] values = plainInt64Values(valueCount);
        ParquetFormat.writePageHeader(chunk, v1FlatHeader(valueCount, values.length));
        chunk.writeBytes(values);
    }

    private static PageHeader v1FlatHeader(int valueCount, int payloadByteSize) {
        return v1FlatHeader(valueCount, payloadByteSize, Optional.empty());
    }

    private static PageHeader v1FlatHeader(int valueCount, int payloadByteSize, Optional<Statistics> statistics) {
        DataPageHeader v1 = new DataPageHeader(valueCount, Encoding.PLAIN, Encoding.RLE, Encoding.RLE, statistics);
        return new PageHeader(
                PageType.DATA_PAGE,
                payloadByteSize,
                payloadByteSize,
                OptionalInt.empty(),
                Optional.of(v1),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    /** A chunk of one V1 data page with an empty payload and a header declaring {@code declaredValueCount} values. */
    static MemorySegment v1PageDeclaring(int declaredValueCount) {
        ByteArrayOutputStream chunk = new ByteArrayOutputStream();
        ParquetFormat.writePageHeader(chunk, v1FlatHeader(declaredValueCount, 0));
        return MemorySegment.ofArray(chunk.toByteArray()).asReadOnly();
    }

    /** A chunk of one V1 data page with an empty payload and a header declaring {@code uncompressedPageSize} bytes. */
    static MemorySegment v1PageDeclaringSize(int uncompressedPageSize) {
        DataPageHeader v1 = new DataPageHeader(1, Encoding.PLAIN, Encoding.RLE, Encoding.RLE, Optional.empty());
        PageHeader header = new PageHeader(
                PageType.DATA_PAGE,
                uncompressedPageSize,
                0,
                OptionalInt.empty(),
                Optional.of(v1),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        ByteArrayOutputStream chunk = new ByteArrayOutputStream();
        ParquetFormat.writePageHeader(chunk, header);
        return MemorySegment.ofArray(chunk.toByteArray()).asReadOnly();
    }

    /**
     * A chunk of one V1 data page with an empty payload and a header declaring one value. The statistics of the header
     * record a {@code min_value} of {@code minValueBytes} zero bytes, and no other bound.
     */
    static MemorySegment v1PageWithMinValue(int minValueBytes) {
        MemorySegment minValue = MemorySegment.ofArray(new byte[minValueBytes]);
        Statistics statistics = new Statistics(
                MemorySegment.NULL,
                MemorySegment.NULL,
                OptionalLong.empty(),
                OptionalLong.empty(),
                MemorySegment.NULL,
                minValue,
                false,
                false,
                OptionalLong.empty());
        ByteArrayOutputStream chunk = new ByteArrayOutputStream();
        ParquetFormat.writePageHeader(chunk, v1FlatHeader(1, 0, Optional.of(statistics)));
        return MemorySegment.ofArray(chunk.toByteArray()).asReadOnly();
    }

    /** A chunk of one V2 data page with an empty payload and a header declaring the given counts. */
    static MemorySegment v2PageDeclaring(int numValues, int numNulls, int numRows) {
        DataPageHeaderV2 v2 =
                new DataPageHeaderV2(numValues, numNulls, numRows, Encoding.PLAIN, 0, 0, false, Optional.empty());
        PageHeader header = new PageHeader(
                PageType.DATA_PAGE_V2,
                0,
                0,
                OptionalInt.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(v2));
        ByteArrayOutputStream chunk = new ByteArrayOutputStream();
        ParquetFormat.writePageHeader(chunk, header);
        return MemorySegment.ofArray(chunk.toByteArray()).asReadOnly();
    }

    /** A chunk of one dictionary page holding {@code entryCount} plain-encoded INT64 entries. */
    static MemorySegment dictionaryPage(int entryCount) {
        byte[] entries = plainInt64Values(entryCount);
        DictionaryPageHeader dictionary = new DictionaryPageHeader(entryCount, Encoding.PLAIN, false);
        PageHeader header = new PageHeader(
                PageType.DICTIONARY_PAGE,
                entries.length,
                entries.length,
                OptionalInt.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(dictionary),
                Optional.empty());
        ByteArrayOutputStream chunk = new ByteArrayOutputStream();
        ParquetFormat.writePageHeader(chunk, header);
        chunk.writeBytes(entries);
        return MemorySegment.ofArray(chunk.toByteArray()).asReadOnly();
    }

    /** One chunk holding the bytes of {@code part}, {@code times} times in a row. */
    static MemorySegment repeated(MemorySegment part, int times) {
        long partBytes = part.byteSize();
        MemorySegment chunk = MemorySegment.ofArray(new byte[Math.toIntExact(partBytes * times)]);
        for (int copy = 0; copy < times; copy++) {
            MemorySegment.copy(part, 0, chunk, copy * partBytes, partBytes);
        }
        return chunk.asReadOnly();
    }

    /** One chunk holding the bytes of {@code parts}, one part after the other. */
    static MemorySegment concat(MemorySegment... parts) {
        ByteArrayOutputStream chunk = new ByteArrayOutputStream();
        for (MemorySegment part : parts) {
            chunk.writeBytes(part.toArray(JAVA_BYTE));
        }
        return MemorySegment.ofArray(chunk.toByteArray()).asReadOnly();
    }

    private static byte[] plainInt64Values(int valueCount) {
        ByteBuffer values = ByteBuffer.allocate(valueCount * Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int value = 0; value < valueCount; value++) {
            values.putLong(value);
        }
        return values.array();
    }
}
