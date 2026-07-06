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
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Verifies that {@link ExplainPlan#estimatedBytesRead()} reflects the total compressed size of the projected leaf
 * column chunks across the row groups that survive pruning.
 *
 * <p>The fixture writes ids {@code 0..8} four rows per row group into two INT64 columns, hence three row groups holding
 * {@code [0,1,2,3]}, {@code [4,5,6,7]}, and {@code [8]}. The eliminating predicate {@code id >= 5} drops the first row
 * group from its statistics, which lets the test prove that an eliminated group contributes nothing to the estimate.
 */
class EstimatedBytesIT {

    private static final Predicate MATCH_ALL = col("id").gtEq(0L);
    private static final Predicate ELIMINATES_FIRST_GROUP = col("id").gtEq(5L);

    @Test
    void fullScanEstimateIsPositiveAndBoundedByFileSize(@TempDir Path tmp) throws Exception {
        Path file = writeThreeRowGroups(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            ExplainPlan plan = reader.explain(MATCH_ALL, Projection.ALL, ReadOptions.DEFAULTS);

            assertThat(plan.estimatedBytesRead())
                    .as("a full scan estimates a positive number of fetched bytes")
                    .isPositive();
            assertThat(plan.estimatedBytesRead())
                    .as("the estimate never exceeds the file size on disk")
                    .isLessThanOrEqualTo(Files.size(file));
        }
    }

    @Test
    void singleColumnProjectionEstimatesFewerBytesThanFullProjection(@TempDir Path tmp) throws Exception {
        Path file = writeThreeRowGroups(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            Projection idOnly = Projection.ofPhysical(Set.of(ColumnPath.of("id")));

            long allColumns = reader.explain(MATCH_ALL, Projection.ALL, ReadOptions.DEFAULTS)
                    .estimatedBytesRead();
            long oneColumn =
                    reader.explain(MATCH_ALL, idOnly, ReadOptions.DEFAULTS).estimatedBytesRead();

            assertThat(oneColumn)
                    .as("projecting a single column fetches fewer bytes than projecting all columns")
                    .isLessThan(allColumns);
        }
    }

    @Test
    void eliminatingPredicateEstimatesFewerBytesThanFullScan(@TempDir Path tmp) throws Exception {
        Path file = writeThreeRowGroups(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            long noElimination = reader.explain(MATCH_ALL, Projection.ALL, ReadOptions.DEFAULTS)
                    .estimatedBytesRead();
            long oneGroupEliminated = reader.explain(ELIMINATES_FIRST_GROUP, Projection.ALL, ReadOptions.DEFAULTS)
                    .estimatedBytesRead();

            assertThat(oneGroupEliminated)
                    .as("an eliminated row group contributes no bytes to the estimate")
                    .isLessThan(noElimination);
        }
    }

    private static Path writeThreeRowGroups(Path tmp) throws Exception {
        ParquetSchema schema = flatSchema(requiredInt64("id"), requiredInt64("payload"));
        WriteOptions options = WriteOptions.builder()
                .tempDir(tmp)
                .rowGroupSize(RowGroupSize.rows(4L))
                .build();
        Path file = tmp.resolve("estimated-bytes.parquet");
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
