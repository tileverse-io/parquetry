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
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.lang.foreign.Arena;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.LevelMaxima;

/**
 * The page walk yields a page's header before its payload is decompressed: a page whose rows are all dead is stepped
 * over as pruned without ever being decoded, and the page decoded after it still opens at the right row of the row
 * group.
 */
class PageCursorPeekTest {

    private static final ColumnPath V = ColumnPath.of("v");
    private static final LevelMaxima FLAT = new LevelMaxima(0, 0);
    private static final LevelMaxima REPEATED = new LevelMaxima(1, 1);

    @Test
    void aPageDiscardedUnreadCountsAsPrunedAndAdvancesTheRowWalk() throws Exception {
        PageCursor cursor = new PageCursor(PageFixtures.v1FlatChunk(5, 7, 4), V, null);
        discardNextPage(cursor);
        discardNextPage(cursor);

        PageCursor.PendingDataPage third = cursor.peekNextDataPage(FLAT);
        assertThat(third.statedRowCount())
                .as("a flat V1 header states its row count as its value count")
                .isEqualTo(4);
        try (Arena arena = Arena.ofConfined()) {
            DecodedPage decoded = cursor.decodePending(third, FLAT, Compression.uncompressed(), arena);
            assertThat(decoded.valueCount())
                    .as("the third page decodes its own values")
                    .isEqualTo(4);
        }

        assertThat(cursor.decodedDataPageCount())
                .as("only the page a window needed was decompressed")
                .isEqualTo(1);
        assertThat(cursor.skippedDataPageCount())
                .as("the two pages stepped over count as pruned")
                .isEqualTo(2);
        assertThat(cursor.currentPageFirstRowIndex())
                .as("the decoded page opens past the rows the discarded pages stated")
                .isEqualTo(12L);
    }

    @Test
    void peekingAPageDecodesNothing() {
        PageCursor cursor = new PageCursor(PageFixtures.v1FlatChunk(5, 7), V, null);

        PageCursor.PendingDataPage first = cursor.peekNextDataPage(FLAT);

        assertThat(first.ordinal()).isZero();
        assertThat(first.firstRowIndex()).isZero();
        assertThat(cursor.decodedDataPageCount()).isZero();
        assertThat(cursor.skippedDataPageCount()).isZero();
    }

    @Test
    void aWalkHoldingAPeekedPageStillHasBytesToDecode() {
        PageCursor cursor = new PageCursor(PageFixtures.v1FlatChunk(5), V, null);

        cursor.peekNextDataPage(FLAT);

        assertThat(cursor.hasRemaining())
                .as("the only page of the chunk is peeked and not yet resolved")
                .isTrue();
    }

    @Test
    void aSecondPeekBeforeTheFirstIsResolvedIsRejected() {
        PageCursor cursor = new PageCursor(PageFixtures.v1FlatChunk(5, 7), V, null);
        cursor.peekNextDataPage(FLAT);

        assertThatIllegalStateException()
                .isThrownBy(() -> cursor.peekNextDataPage(FLAT))
                .withMessageContaining("unresolved");
    }

    @Test
    void aPageStatingNoRowCountCannotBeDiscardedUnread() {
        PageCursor cursor = new PageCursor(PageFixtures.v1FlatChunk(5), V, null);

        PageCursor.PendingDataPage pending = cursor.peekNextDataPage(REPEATED);

        assertThat(pending.statesRowCount())
                .as("a V1 header of a repeated column states neither its rows nor its row count")
                .isFalse();
        assertThatIllegalStateException()
                .isThrownBy(() -> cursor.discardPending(pending))
                .withMessageContaining("states no row count");
    }

    @Test
    void theWholePageReadWalksTheChunkAsBefore() throws Exception {
        PageCursor cursor = new PageCursor(PageFixtures.v1FlatChunk(5, 7), V, null);

        try (Arena arena = Arena.ofConfined()) {
            assertThat(cursor.nextDataPage(FLAT, Compression.uncompressed(), arena)
                            .valueCount())
                    .isEqualTo(5);
            cursor.recordCurrentPageRowCount(5);
            assertThat(cursor.nextDataPage(FLAT, Compression.uncompressed(), arena)
                            .valueCount())
                    .isEqualTo(7);
            assertThat(cursor.currentPageFirstRowIndex()).isEqualTo(5L);
            assertThat(cursor.nextDataPage(FLAT, Compression.uncompressed(), arena))
                    .isNull();
        }
        assertThat(cursor.decodedDataPageCount()).isEqualTo(2);
    }

    private static void discardNextPage(PageCursor cursor) {
        cursor.discardPending(cursor.peekNextDataPage(FLAT));
    }
}
