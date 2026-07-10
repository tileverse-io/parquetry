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
package io.tileverse.parquetry.internal.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.observe.FetchAccumulator;
import io.tileverse.parquetry.observe.FetchStats;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/** Verifies that a page fetch records its byte count into the active {@link FetchAccumulator}. */
class RowGroupFetcherFetchStatsTest {

    private static final ColumnPath V = ColumnPath.of("v");

    @TempDir
    Path tempDir;

    @Test
    void fetchRecordsPageBytesIntoTheActiveAccumulator() throws Exception {
        Path file = writeRows();
        ParquetSchema schema = flatSchema();
        FetchAccumulator accumulator = FetchAccumulator.active();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            RowGroupSurvivor survivor = survivorFor(source, schema);
            RowGroupFetcher fetcher = TestFetchers.over(source, schema, schema, SegmentPool.getDefault(), accumulator);
            FetchPlan plan = fetcher.planFor(survivor);

            try (RowGroupFetch fetch = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                FetchStats stats = accumulator.snapshot();
                assertThat(stats.pageBytes())
                        .as("page bytes recorded match the plan's total range bytes")
                        .isEqualTo(plan.totalBytes());
                assertThat(stats.fetchCount())
                        .as("at least one coalesced range was fetched")
                        .isGreaterThanOrEqualTo(1);
            }
        }
    }

    @Test
    void fetchWithTheNoOpAccumulatorRecordsNothing() throws Exception {
        Path file = writeRows();
        ParquetSchema schema = flatSchema();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            RowGroupSurvivor survivor = survivorFor(source, schema);
            RowGroupFetcher fetcher =
                    TestFetchers.over(source, schema, schema, SegmentPool.getDefault(), FetchAccumulator.NONE);

            try (RowGroupFetch fetch = fetcher.fetch(survivor, fetcher.planFor(survivor), BudgetReservation.NONE)) {
                assertThat(fetch.columns())
                        .as("the fetch still reads its column chunk")
                        .hasSize(1);
            }
            assertThat(FetchAccumulator.NONE.snapshot())
                    .as("the no-op accumulator stays empty")
                    .isEqualTo(FetchStats.EMPTY);
        }
    }

    private static RowGroupSurvivor survivorFor(ByteRangeSource source, ParquetSchema schema) {
        FileMetaData footer = ParquetFormat.readFooter(source);
        RowGroup rowGroup = footer.rowGroups().get(0);
        RowGroupChunks chunks = RowGroupChunks.of(rowGroup, schema, indexLoaderNotUsed());
        return RowGroupSurvivor.full(chunks);
    }

    private static IndexSectionLoader indexLoaderNotUsed() {
        return new IndexSectionLoader() {
            @Override
            public io.tileverse.parquetry.format.OffsetIndex readOffsetIndex(long offset, int length) {
                throw new UnsupportedOperationException("index sections are not read in this test");
            }

            @Override
            public io.tileverse.parquetry.format.ColumnIndex readColumnIndex(long offset, int length) {
                throw new UnsupportedOperationException("index sections are not read in this test");
            }

            @Override
            public io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter readBloom(
                    long offset, int length) {
                throw new UnsupportedOperationException("index sections are not read in this test");
            }
        };
    }

    private Path writeRows() throws Exception {
        Path file = tempDir.resolve("fetch-stats.parquet");
        ParquetSchema schema = flatSchema();
        WriteOptions options = WriteOptions.builder().tempDir(tempDir).build();
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (int v = 0; v < 64; v++) {
            rows.add(Map.of(V, v));
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.writeBatch(WriteFixtures.batch(schema, rows));
        }
        return file;
    }

    private static ParquetSchema flatSchema() {
        SchemaNode.Primitive leaf = new SchemaNode.Primitive(
                "v", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        List<SchemaNode> children = Stream.<SchemaNode>of(leaf).toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
