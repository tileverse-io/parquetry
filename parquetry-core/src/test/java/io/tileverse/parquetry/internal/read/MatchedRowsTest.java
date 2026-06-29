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
package io.tileverse.parquetry.internal.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.RowRanges.Range;

class MatchedRowsTest {

    @Test
    void passedRowsCoalesceIntoContiguousRanges() {
        // Rows 3,4,5 form one run; row 9 is isolated.
        MatchedRows.Builder builder = MatchedRows.builder();
        builder.accept(3, true);
        builder.accept(4, true);
        builder.accept(5, true);
        builder.accept(6, false);
        builder.accept(9, true);
        MatchedRows selection = builder.build();

        List<Range> ranges = selection.rows().ranges();
        assertThat(ranges).as("expected two coalesced ranges").containsExactly(new Range(3, 5), new Range(9, 9));
        assertThat(selection.rows().totalRows()).as("total passed rows").isEqualTo(4);
        assertThat(selection.isEmpty()).as("selection is not empty").isFalse();
    }

    @Test
    void allSurvivingRowsPassProducesSingleRange() {
        MatchedRows.Builder builder = MatchedRows.builder();
        builder.accept(2, true);
        builder.accept(3, true);
        builder.accept(4, true);
        MatchedRows selection = builder.build();

        RowRanges rows = selection.rows();
        assertThat(rows.ranges()).as("single contiguous range").containsExactly(new Range(2, 4));
        assertThat(rows.totalRows()).as("total rows").isEqualTo(3);
    }

    @Test
    void noRowsPassProducesEmptySelection() {
        MatchedRows.Builder builder = MatchedRows.builder();
        builder.accept(1, false);
        builder.accept(2, false);
        MatchedRows selection = builder.build();

        assertThat(selection.isEmpty()).as("isEmpty when no rows passed").isTrue();
        assertThat(selection.rows().totalRows()).as("totalRows when empty").isZero();
    }

    @Test
    void passedRowsSeparatedByGapsStaySeparateRanges() {
        // Gaps wider than one (5 follows 3, 9 follows 5) must not coalesce, even with no explicit failing row between.
        MatchedRows.Builder builder = MatchedRows.builder();
        builder.accept(3, true);
        builder.accept(5, true);
        builder.accept(9, true);
        MatchedRows selection = builder.build();

        assertThat(selection.rows().ranges())
                .as("non-adjacent passed rows form one singleton range each")
                .containsExactly(new Range(3, 3), new Range(5, 5), new Range(9, 9));
        assertThat(selection.rows().totalRows()).as("total passed rows").isEqualTo(3);
    }

    @Test
    void singlePassedRowProducesSingletonRange() {
        MatchedRows.Builder builder = MatchedRows.builder();
        builder.accept(7, true);
        MatchedRows selection = builder.build();

        assertThat(selection.rows().ranges()).as("singleton range").containsExactly(new Range(7, 7));
        assertThat(selection.rows().totalRows()).as("total rows").isEqualTo(1);
        assertThat(selection.isEmpty()).as("not empty").isFalse();
    }
}
