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
package io.tileverse.parquetry.dataset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.SequencedSet;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Query;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

/**
 * The output shape may rename a physical column. When the query's predicate references the presented (renamed) column,
 * the dataset must lower it to the physical column name before file-level pushdown, otherwise the pushdown rejects the
 * unknown presented name. These tests pin that lowering for {@code read(Query)} and {@code readBatches(Query)}.
 */
class ParquetSourceRenameOutputTest {

    private static final Path FILE = CorpusFixtures.parquetTestingData().resolve("alltypes_plain.parquet");

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath RECORD_ID = ColumnPath.of("record_id");

    @Test
    void readWithRenameOutputLowersPredicateToPhysicalName() throws Exception {
        try (ByteRangeSource bytes = ByteRangeSource.ofFile(FILE)) {
            ParquetSource source = ParquetSource.open(bytes);
            Query renamed = renameIdToRecordIdQuery(presentedIdGreaterThan(4));

            List<Integer> presented = readPresentedIds(source, renamed);

            assertThat(presented).isNotEmpty().allMatch(value -> value > 4);
            assertThat(presented).containsExactlyElementsOf(physicalIdsGreaterThan(source, 4));
        }
    }

    @Test
    void readBatchesWithRenameOutputDoesNotThrow() throws Exception {
        try (ByteRangeSource bytes = ByteRangeSource.ofFile(FILE)) {
            ParquetSource source = ParquetSource.open(bytes);
            Query renamed = renameIdToRecordIdQuery(presentedIdGreaterThan(4));

            assertThatCode(() -> {
                        try (Stream<?> batches = source.readBatches(renamed, ReadOptions.DEFAULTS)) {
                            batches.forEach(batch -> {});
                        }
                    })
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void shapedReadReturnsExactlyTheMatchingRows() throws Exception {
        try (ByteRangeSource bytes = ByteRangeSource.ofFile(FILE)) {
            ParquetSource source = ParquetSource.open(bytes);
            Query renamed = renameIdToRecordIdQuery(presentedIdGreaterThan(4));

            long total = source.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);
            long matching = source.count(renamed, ReadOptions.DEFAULTS);
            long shapedRowCount = readPresentedIds(source, renamed).size();

            assertThat(matching).isPositive().isLessThan(total);
            assertThat(shapedRowCount).isEqualTo(matching);
        }
    }

    @Test
    void countWithRenameOutputLowersPredicateToPhysicalName() throws Exception {
        try (ByteRangeSource bytes = ByteRangeSource.ofFile(FILE)) {
            ParquetSource source = ParquetSource.open(bytes);
            Query renamed = renameIdToRecordIdQuery(presentedIdGreaterThan(4));

            long baseline = source.count(physicalIdGreaterThan(4), ReadOptions.DEFAULTS);

            assertThat(source.count(renamed, ReadOptions.DEFAULTS)).isEqualTo(baseline);
        }
    }

    @Test
    void explainWithRenameOutputLowersPredicateToPhysicalName() throws Exception {
        try (ByteRangeSource bytes = ByteRangeSource.ofFile(FILE)) {
            ParquetSource source = ParquetSource.open(bytes);
            Query renamed = renameIdToRecordIdQuery(presentedIdGreaterThan(4));

            ExplainPlan baseline = source.explain(physicalIdGreaterThan(4), renamed.projection(), ReadOptions.DEFAULTS);
            ExplainPlan plan = source.explain(renamed, ReadOptions.DEFAULTS);

            assertThat(plan.rowGroups()).hasSameSizeAs(baseline.rowGroups());
            assertThat(plan.estimatedRowsScanned()).isEqualTo(baseline.estimatedRowsScanned());
        }
    }

    @Test
    void explainAnalyzeWithRenameOutputDoesNotThrow() throws Exception {
        try (ByteRangeSource bytes = ByteRangeSource.ofFile(FILE)) {
            ParquetSource source = ParquetSource.open(bytes);
            Query renamed = renameIdToRecordIdQuery(presentedIdGreaterThan(4));

            assertThatCode(() -> source.explainAnalyze(renamed, ReadOptions.DEFAULTS))
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void emptyOutputIdentityCaseIsUnchanged() throws Exception {
        try (ByteRangeSource bytes = ByteRangeSource.ofFile(FILE)) {
            ParquetSource source = ParquetSource.open(bytes);
            Query identity = Query.of(physicalIdGreaterThan(4), Projection.ALL);

            List<Integer> identityIds = readPhysicalIds(source, identity);

            assertThat(identityIds).containsExactlyElementsOf(physicalIdsGreaterThan(source, 4));
        }
    }

    private static Predicate presentedIdGreaterThan(int threshold) {
        return new Predicate.Gt(RECORD_ID, new Value.IntVal(threshold));
    }

    private static Predicate physicalIdGreaterThan(int threshold) {
        return new Predicate.Gt(ID, new Value.IntVal(threshold));
    }

    private static Query renameIdToRecordIdQuery(Predicate predicate) {
        SequencedSet<Projection.Column> columns =
                new LinkedHashSet<>(List.of(new Projection.Column.Physical(RECORD_ID, ID)));
        return Query.of(predicate, Projection.of(columns));
    }

    private static List<Integer> readPresentedIds(ParquetSource source, Query query) {
        try (Stream<ParquetRecord> rows = source.read(query, ReadOptions.DEFAULTS)) {
            return rows.map(row -> row.getInt(RECORD_ID)).toList();
        }
    }

    private static List<Integer> readPhysicalIds(ParquetSource source, Query query) {
        try (Stream<ParquetRecord> rows = source.read(query, ReadOptions.DEFAULTS)) {
            return rows.map(row -> row.getInt(ID)).toList();
        }
    }

    private static List<Integer> physicalIdsGreaterThan(ParquetSource source, int threshold) {
        try (Stream<ParquetRecord> rows = source.read(
                physicalIdGreaterThan(threshold), Projection.ofPhysical(Set.of(ID)), ReadOptions.DEFAULTS)) {
            return rows.map(row -> row.getInt(ID)).toList();
        }
    }
}
