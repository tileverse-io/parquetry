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

import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.format.DataPageHeader;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormat;

/** Builds column-chunk bytes for a {@link PageCursor} walk directly, without writing a Parquet file. */
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
        DataPageHeader v1 =
                new DataPageHeader(valueCount, Encoding.PLAIN, Encoding.RLE, Encoding.RLE, Optional.empty());
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

    private static byte[] plainInt64Values(int valueCount) {
        ByteBuffer values = ByteBuffer.allocate(valueCount * Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int value = 0; value < valueCount; value++) {
            values.putLong(value);
        }
        return values.array();
    }
}
