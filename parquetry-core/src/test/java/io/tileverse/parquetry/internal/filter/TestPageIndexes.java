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
package io.tileverse.parquetry.internal.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * Builds {@link PageIndex} instances for tests. The pages are 100 bytes each, placed back to back from offset 0 in a
 * chunk holding nothing else.
 */
public final class TestPageIndexes {

    /** The size of each page, header included. */
    public static final int PAGE_BYTES = 100;

    private TestPageIndexes() {}

    /**
     * The pages starting at {@code firstRows} in a row group of {@code rowCount} rows, without statistics.
     *
     * @throws IllegalArgumentException when the pages fail validation
     */
    public static PageIndex pages(long rowCount, long... firstRows) {
        OffsetIndex offsetIndex = offsetIndex(firstRows);
        long chunkLength = (long) PAGE_BYTES * firstRows.length;
        Optional<PageIndex> declared = PageIndex.declared(offsetIndex, 0L, chunkLength, rowCount);
        return declared.orElseThrow(() -> new IllegalArgumentException("pages failing validation"));
    }

    /** An offset index placing pages of {@link #PAGE_BYTES} bytes back to back from offset 0, at {@code firstRows}. */
    public static OffsetIndex offsetIndex(long... firstRows) {
        List<PageLocation> pages = new ArrayList<>(firstRows.length);
        for (int page = 0; page < firstRows.length; page++) {
            pages.add(new PageLocation((long) PAGE_BYTES * page, PAGE_BYTES, firstRows[page]));
        }
        return new OffsetIndex(pages, Optional.empty());
    }

    /**
     * The page index declared by the writer for column {@code column} of {@code rowGroup}, read from {@code source}
     * without statistics.
     *
     * @throws IllegalArgumentException when the pages fail validation
     */
    public static PageIndex readDeclared(ByteRangeSource source, RowGroup rowGroup, int column) {
        ColumnChunk chunk = rowGroup.columns().get(column);
        ColumnMetaData meta = chunk.metaData().orElseThrow();
        OffsetIndex offsetIndex = ParquetFormat.readOffsetIndex(
                source,
                chunk.offsetIndexOffset().getAsLong(),
                chunk.offsetIndexLength().getAsInt());
        Optional<PageIndex> declared =
                PageIndex.declared(offsetIndex, chunkStart(meta), meta.totalCompressedSize(), rowGroup.numRows());
        return declared.orElseThrow(() -> new IllegalArgumentException("pages failing validation"));
    }

    /** The first byte of a chunk: its dictionary page when it precedes the first data page, else that data page. */
    private static long chunkStart(ColumnMetaData meta) {
        long dataPageOffset = meta.dataPageOffset();
        long dictionaryPageOffset = meta.dictionaryPageOffset().orElse(0L);
        boolean dictionaryPrecedesData = dictionaryPageOffset > 0 && dictionaryPageOffset < dataPageOffset;
        return dictionaryPrecedesData ? dictionaryPageOffset : dataPageOffset;
    }
}
