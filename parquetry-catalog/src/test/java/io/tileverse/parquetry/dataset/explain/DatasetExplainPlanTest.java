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
package io.tileverse.parquetry.dataset.explain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Predicate;

class DatasetExplainPlanTest {

    @Test
    void totalsCountFilesAndSumRowsWhenAllReport() {
        FileExplain kept = new FileExplain("a.parquet", Outcome.KEEP, "kept", OptionalLong.of(10), Optional.empty());
        FileExplain skipped =
                new FileExplain("b.parquet", Outcome.SKIP, "bounds disjoint", OptionalLong.of(90), Optional.empty());

        Totals totals = Totals.from(List.of(kept, skipped));

        assertThat(totals.filesTotal()).isEqualTo(2);
        assertThat(totals.filesKept()).isEqualTo(1);
        assertThat(totals.filesSkipped()).isEqualTo(1);
        assertThat(totals.rowsTotal()).hasValue(100);
        assertThat(totals.rowsSkipped()).hasValue(90);
    }

    @Test
    void rowTotalsAreUnknownWhenAnyFileOmitsRecordCount() {
        FileExplain known = new FileExplain("a.parquet", Outcome.KEEP, "kept", OptionalLong.of(10), Optional.empty());
        FileExplain unknown =
                new FileExplain("b.parquet", Outcome.SKIP, "skipped", OptionalLong.empty(), Optional.empty());

        Totals totals = Totals.from(List.of(known, unknown));

        assertThat(totals.rowsTotal()).isEmpty();
        assertThat(totals.rowsSkipped()).isEmpty();
    }

    @Test
    void asciiTableShowsOutcomesAndReasons() {
        FileExplain kept = new FileExplain("a.parquet", Outcome.KEEP, "kept", OptionalLong.of(10), Optional.empty());
        FileExplain skipped =
                new FileExplain("b.parquet", Outcome.SKIP, "bounds disjoint", OptionalLong.of(90), Optional.empty());
        DatasetExplainPlan plan = new DatasetExplainPlan(
                Predicate.ALWAYS_TRUE, List.of(kept, skipped), Totals.from(List.of(kept, skipped)));

        String table = plan.toAsciiTable();

        assertThat(table).contains("a.parquet", "KEEP", "b.parquet", "SKIP", "bounds disjoint");
        assertThat(table).contains("1 kept", "1 skipped");
    }

    @Test
    void jsonHasFilesAndTotals() {
        FileExplain skipped =
                new FileExplain("b.parquet", Outcome.SKIP, "bounds disjoint", OptionalLong.of(90), Optional.empty());
        DatasetExplainPlan plan =
                new DatasetExplainPlan(Predicate.ALWAYS_TRUE, List.of(skipped), Totals.from(List.of(skipped)));

        String json = plan.toJson();

        assertThat(json).contains("\"files\"", "\"outcome\":\"SKIP\"", "\"location\":\"b.parquet\"", "\"totals\"");
    }

    @Test
    void jsonEscapesNewlineInReason() {
        FileExplain skipped =
                new FileExplain("b.parquet", Outcome.SKIP, "line one\nline two", OptionalLong.of(90), Optional.empty());
        DatasetExplainPlan plan =
                new DatasetExplainPlan(Predicate.ALWAYS_TRUE, List.of(skipped), Totals.from(List.of(skipped)));

        String json = plan.toJson();

        assertThat(json).contains("line one\\nline two").doesNotContain("line one\nline two");
    }
}
