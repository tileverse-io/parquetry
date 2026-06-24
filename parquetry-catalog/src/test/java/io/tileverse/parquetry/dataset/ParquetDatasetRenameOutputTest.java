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
package io.tileverse.parquetry.dataset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.OutputColumn;
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
class ParquetDatasetRenameOutputTest {

    private static final Path FILE = CorpusFixtures.parquetTestingData().resolve("alltypes_plain.parquet");

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath RECORD_ID = ColumnPath.of("record_id");

    @Test
    void readWithRenameOutputLowersPredicateToPhysicalName() throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(FILE)) {
            ParquetDataset dataset = ParquetDataset.open(source);
            Query renamed = renameIdToRecordIdQuery(presentedIdGreaterThan(4));

            List<Integer> presented = readPresentedIds(dataset, renamed);

            assertThat(presented).isNotEmpty().allMatch(value -> value > 4);
            assertThat(presented).containsExactlyElementsOf(physicalIdsGreaterThan(dataset, 4));
        }
    }

    @Test
    void readBatchesWithRenameOutputDoesNotThrow() throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(FILE)) {
            ParquetDataset dataset = ParquetDataset.open(source);
            Query renamed = renameIdToRecordIdQuery(presentedIdGreaterThan(4));

            assertThatCode(() -> {
                        try (Stream<?> batches = dataset.readBatches(renamed, ReadOptions.DEFAULTS)) {
                            batches.forEach(batch -> {});
                        }
                    })
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void countWithRenameOutputLowersPredicateToPhysicalName() throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(FILE)) {
            ParquetDataset dataset = ParquetDataset.open(source);
            Query renamed = renameIdToRecordIdQuery(presentedIdGreaterThan(4));

            long baseline = dataset.count(physicalIdGreaterThan(4), ReadOptions.DEFAULTS);

            assertThat(dataset.count(renamed, ReadOptions.DEFAULTS)).isEqualTo(baseline);
        }
    }

    @Test
    void explainWithRenameOutputLowersPredicateToPhysicalName() throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(FILE)) {
            ParquetDataset dataset = ParquetDataset.open(source);
            Query renamed = renameIdToRecordIdQuery(presentedIdGreaterThan(4));

            ExplainPlan baseline =
                    dataset.explain(physicalIdGreaterThan(4), renamed.projection(), ReadOptions.DEFAULTS);
            ExplainPlan plan = dataset.explain(renamed, ReadOptions.DEFAULTS);

            assertThat(plan.rowGroups()).hasSameSizeAs(baseline.rowGroups());
            assertThat(plan.estimatedRowsScanned()).isEqualTo(baseline.estimatedRowsScanned());
        }
    }

    @Test
    void explainAnalyzeWithRenameOutputDoesNotThrow() throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(FILE)) {
            ParquetDataset dataset = ParquetDataset.open(source);
            Query renamed = renameIdToRecordIdQuery(presentedIdGreaterThan(4));

            assertThatCode(() -> dataset.explainAnalyze(renamed, ReadOptions.DEFAULTS))
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void emptyOutputIdentityCaseIsUnchanged() throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(FILE)) {
            ParquetDataset dataset = ParquetDataset.open(source);
            Query identity = Query.of(physicalIdGreaterThan(4), Projection.ALL);

            List<Integer> identityIds = readPhysicalIds(dataset, identity);

            assertThat(identityIds).containsExactlyElementsOf(physicalIdsGreaterThan(dataset, 4));
        }
    }

    private static Predicate presentedIdGreaterThan(int threshold) {
        return new Predicate.Gt(RECORD_ID, new Value.IntVal(threshold));
    }

    private static Predicate physicalIdGreaterThan(int threshold) {
        return new Predicate.Gt(ID, new Value.IntVal(threshold));
    }

    private static Query renameIdToRecordIdQuery(Predicate predicate) {
        List<OutputColumn> output = List.of(new OutputColumn.Physical(RECORD_ID, ID));
        return new Query(predicate, Projection.of(Set.of(ID)), output);
    }

    private static List<Integer> readPresentedIds(ParquetDataset dataset, Query query) {
        try (Stream<ParquetRecord> rows = dataset.read(query, ReadOptions.DEFAULTS)) {
            return rows.map(row -> row.getInt(RECORD_ID)).toList();
        }
    }

    private static List<Integer> readPhysicalIds(ParquetDataset dataset, Query query) {
        try (Stream<ParquetRecord> rows = dataset.read(query, ReadOptions.DEFAULTS)) {
            return rows.map(row -> row.getInt(ID)).toList();
        }
    }

    private static List<Integer> physicalIdsGreaterThan(ParquetDataset dataset, int threshold) {
        try (Stream<ParquetRecord> rows =
                dataset.read(physicalIdGreaterThan(threshold), Projection.of(Set.of(ID)), ReadOptions.DEFAULTS)) {
            return rows.map(row -> row.getInt(ID)).toList();
        }
    }
}
