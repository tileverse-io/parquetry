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
package io.tileverse.parquetry.data;

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.RowGroupPlan;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.observe.QueryObserver;
import io.tileverse.parquetry.observe.QueryStarted;
import io.tileverse.parquetry.observe.QueryStats;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Verifies that {@link ParquetFileReader#explainAnalyze} annotates the explain plan with the real execution stats of
 * one count-style drain: the matched-row count and the fetched-byte total measured during the drain, plus the
 * per-row-group read records.
 *
 * <p>The fixture writes ids {@code 0..8} four rows per row group, hence three row groups holding {@code [0,1,2,3]},
 * {@code [4,5,6,7]}, and {@code [8]}. The predicate {@code id >= 5} eliminates the first row group from statistics,
 * fully matches the third, and partially matches the second, which exercises both the metadata-only and the decoded
 * paths.
 */
class ExplainAnalyzeIT {

    private static final Predicate MATCHES_SUBSET = col("id").gtEq(5L);

    @Test
    void analyzeAnnotatesPlanWithRealExecutionStats(@TempDir Path tmp) throws Exception {
        Path file = writeThreeRowGroups(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            long actual = reader.count(MATCHES_SUBSET, ReadOptions.DEFAULTS);
            ExplainPlan plan = reader.explainAnalyze(MATCHES_SUBSET, Projection.ALL, ReadOptions.DEFAULTS);

            assertThat(plan.execution())
                    .as("the plan is annotated with execution stats")
                    .isPresent();
            assertThat(plan.execution().orElseThrow().rowsMatched())
                    .as("the analyzed match count equals count()")
                    .isEqualTo(actual);
            assertThat(plan.execution().orElseThrow().totalFetch().totalBytes())
                    .as("the drain tallies real fetched bytes")
                    .isPositive();
            assertThat(plan.estimatedBytesRead())
                    .as("the planner estimate is a positive upper bound")
                    .isPositive();
            assertThat(plan.toAsciiTable())
                    .as("the ascii table renders the execution columns")
                    .contains("actual");
            assertThat(plan.toJson())
                    .as("the json renders the execution object")
                    .contains("execution");
            assertThat(plan.rowGroups().stream().map(RowGroupPlan::execution).anyMatch(Optional::isPresent))
                    .as("at least one decoded row group has a per-group execution record")
                    .isTrue();
        }
    }

    /**
     * An always-true predicate is a full scan: the analyze drain decodes every projected column for every row and
     * discards the values. The plan comes back annotated with the real execution stats of that scan. Every row is
     * decoded and matched, the drain reads page bytes, and the attached observer sees a paired start/finish.
     */
    @Test
    void alwaysTruePlanAnnotatesFullScanExecution(@TempDir Path tmp) throws Exception {
        Path file = writeThreeRowGroups(tmp);
        long totalRows = 9L;
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            StartFinishCountingObserver observer = new StartFinishCountingObserver();
            ReadOptions options = ReadOptions.builder().queryObserver(observer).build();

            ExplainPlan plan = reader.explainAnalyze(Predicate.ALWAYS_TRUE, Projection.ALL, options);

            assertThat(plan.execution())
                    .as("a full scan decodes the projected columns, hence a real execution annotation")
                    .isPresent();
            assertThat(plan.execution().orElseThrow().rowsDecoded())
                    .as("every row is decoded in a full scan")
                    .isEqualTo(totalRows);
            assertThat(plan.execution().orElseThrow().rowsMatched())
                    .as("an always-true predicate matches every row")
                    .isEqualTo(totalRows);
            assertThat(plan.execution().orElseThrow().totalFetch().totalBytes())
                    .as("the drain tallies real fetched bytes for the projected columns")
                    .isPositive();
            assertThat(observer.startedCount)
                    .as("the drain fires exactly one query-started event")
                    .isEqualTo(1);
            assertThat(observer.finishedCount)
                    .as("the drain fires exactly one query-finished event, paired with the start")
                    .isEqualTo(1);
            assertThat(plan.rowGroups())
                    .as("the plan still reports every row group")
                    .hasSize(3);
        }
    }

    private static final class StartFinishCountingObserver implements QueryObserver {

        private int startedCount;
        private int finishedCount;

        @Override
        public synchronized void onQueryStarted(QueryStarted started) {
            startedCount++;
        }

        @Override
        public synchronized void onQueryFinished(QueryStats stats) {
            finishedCount++;
        }
    }

    private static Path writeThreeRowGroups(Path tmp) throws Exception {
        ParquetSchema schema = flatSchema(requiredInt64("id"), requiredInt64("payload"));
        WriteOptions options = WriteOptions.builder()
                .tempDir(tmp)
                .rowGroupSize(RowGroupSize.rows(4L))
                .build();
        Path file = tmp.resolve("explain-analyze.parquet");
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(1);
            for (int i = 0; i < 9; i++) {
                WriteFixtures.appendRow(
                        appender,
                        schema,
                        Map.of(
                                ColumnPath.of("id"), (long) i,
                                ColumnPath.of("payload"), (long) (i * 1000)));
            }
        }
        return file;
    }

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children = Stream.of(leaves).map(f -> (SchemaNode) f).toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredInt64(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
    }
}
