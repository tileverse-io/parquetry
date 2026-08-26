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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.LevelMaxima;

/**
 * The page walk's row index without an offset index: each page's first row is the running sum of the row counts the
 * preceding pages reported, and a header-declared row count must agree with the count the levels produced.
 */
class PageCursorRowIndexTest {

    private static final ColumnPath V = ColumnPath.of("v");
    private static final LevelMaxima FLAT = new LevelMaxima(0, 0);

    @Test
    void firstRowIndexAccumulatesAcrossPagesWithoutASelection() throws Exception {
        PageCursor cursor = new PageCursor(PageFixtures.v1FlatChunk(5, 7, 4), V, null);
        List<Long> firstRows = new ArrayList<>();
        try (Arena arena = Arena.ofConfined()) {
            for (int page = 0; page < 3; page++) {
                DecodedPage decoded = cursor.nextDataPage(FLAT, Compression.uncompressed(), arena);
                firstRows.add(cursor.currentPageFirstRowIndex());
                cursor.recordCurrentPageRowCount(decoded.valueCount());
            }
        }
        assertThat(firstRows).containsExactly(0L, 5L, 12L);
    }

    @Test
    void aDisagreeingRowCountFailsLoud() throws Exception {
        PageCursor cursor = new PageCursor(PageFixtures.v1FlatChunk(5), V, null);
        try (Arena arena = Arena.ofConfined()) {
            cursor.nextDataPage(FLAT, Compression.uncompressed(), arena);
            assertThatThrownBy(() -> cursor.recordCurrentPageRowCount(4))
                    .isInstanceOf(MalformedFileException.class)
                    .hasMessageContaining("declares 5 rows")
                    .hasMessageContaining("levels hold 4");
        }
    }
}
