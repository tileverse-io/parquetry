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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.EncodingPolicy;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.RowRanges.Range;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.runtime.FetchBudget;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * What a speculative prefetch reserves against the {@link FetchBudget}: the plan's first-to-last byte span, holes
 * included, rather than the bytes requested by the plan. The span is the ceiling on the scratch memory held by a byte
 * source while it serves the plan, and reserving the smaller figure would admit more concurrent prefetches on exactly
 * the narrowed reads where that scratch appears.
 *
 * <p>The fixture holds two row groups of two long columns, ten pages per column per row group, every column written
 * {@code PLAIN}. A mask over the first and last rows of a row group skips the eight pages between, which gives a plan
 * whose span is several times its bytes.
 */
class FetchSpanReservationTest {

    private static final ColumnPath A = ColumnPath.of("a");
    private static final ColumnPath B = ColumnPath.of("b");

    private static final int ROWS_PER_ROW_GROUP = 1_000;
    private static final int PAGE_VALUE_LIMIT = 100;

    /** The row group speculated on by the prefetcher while row group 0 is being taken. */
    private static final int PREFETCHED_ROW_GROUP = 1;

    private static final ParquetSchema SCHEMA = twoLongColumnsSchema();

    @TempDir
    static Path tempDir;

    private static Path file;

    @BeforeAll
    static void writeFixtureFile() throws IOException {
        file = writeTwoRowGroupsOfTenPages();
    }

    @Test
    void aNarrowedPlanSpansTheHolesItLeaves() {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            Fixture fixture = Fixture.open(source);
            FetchPlan plan = fixture.narrowedPlanFor(PREFETCHED_ROW_GROUP);

            assertThat(plan.spanBytes())
                    .as("the first requested byte to the last, whatever the plan skips between them")
                    .isEqualTo(endOfLastRequestedByte(plan) - firstRequestedByte(plan));
            assertThat(plan.spanBytes())
                    .as("the eight skipped pages of each column lie inside the span and outside the bytes")
                    .isGreaterThan(plan.requestedBytes());
        }
    }

    @Test
    void aBudgetBelowTheSpanDeclinesThePrefetch() throws IOException {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            Fixture fixture = Fixture.open(source);
            FetchPlan plan = fixture.narrowedPlanFor(PREFETCHED_ROW_GROUP);
            long capacity = halfwayBetweenBytesAndSpan(plan);
            assertThat(capacity)
                    .as("a budget that fits the plan's bytes and not its span tells the two apart")
                    .isGreaterThan(plan.requestedBytes())
                    .isLessThan(plan.spanBytes());
            FetchBudget budget = FetchBudget.ofBytes(capacity);

            try (RowGroupPrefetcher prefetcher = fixture.prefetcherOver(budget);
                    RowGroupFetch current = prefetcher.take(0)) {
                assertThat(current.columns()).hasSize(2);
                assertThat(budget.available())
                        .as("no speculative fetch was admitted, hence nothing is reserved")
                        .isEqualTo(budget.capacity());
            }
        }
    }

    @Test
    void anAdmittedPrefetchHoldsTheWholeSpan() throws IOException {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            Fixture fixture = Fixture.open(source);
            FetchPlan plan = fixture.narrowedPlanFor(PREFETCHED_ROW_GROUP);
            FetchBudget budget = FetchBudget.ofBytes(plan.spanBytes());

            try (RowGroupPrefetcher prefetcher = fixture.prefetcherOver(budget);
                    RowGroupFetch current = prefetcher.take(0)) {
                assertThat(current.columns()).hasSize(2);
                assertThat(budget.available())
                        .as("the prefetch in flight holds the span, holes included, and not merely its bytes")
                        .isZero();
            }
        }
    }

    // -------------------------------------------------------------------------
    // Plan geometry
    // -------------------------------------------------------------------------

    /** The plan's first requested byte: {@code planFor} orders the units by file offset. */
    private static long firstRequestedByte(FetchPlan plan) {
        return plan.units().getFirst().fileOffset();
    }

    /** The byte just past the plan's last requested one. */
    private static long endOfLastRequestedByte(FetchPlan plan) {
        FetchUnit last = plan.units().getLast();
        return last.fileOffset() + last.length();
    }

    private static long halfwayBetweenBytesAndSpan(FetchPlan plan) {
        long requested = plan.requestedBytes();
        return requested + (plan.spanBytes() - requested) / 2;
    }

    // -------------------------------------------------------------------------
    // Fixture
    // -------------------------------------------------------------------------

    /** The fixture file's two row groups, their chunk views, and a fetcher over them. */
    private record Fixture(List<RowGroupSurvivor> survivors, RowGroupPlans plans, RowGroupFetcher fetcher) {

        static Fixture open(ByteRangeSource source) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            List<RowGroupSurvivor> survivors = new ArrayList<>();
            List<Optional<RowMask>> masks = new ArrayList<>();
            for (RowGroupChunks chunks : TestRowGroupChunks.allOf(footer, SCHEMA, TestRowGroupChunks.reading(source))) {
                survivors.add(RowGroupSurvivor.full(chunks));
                masks.add(Optional.of(maskOverFirstAndLastRows(chunks)));
            }
            RowGroupPlans plans = RowGroupPlans.unplanned(survivors, masks, true);
            RowGroupFetcher fetcher = TestFetchers.over(source, SCHEMA, SCHEMA, SegmentPool.create());
            return new Fixture(survivors, plans, fetcher);
        }

        FetchPlan narrowedPlanFor(int rowGroup) {
            return fetcher.planFor(survivors.get(rowGroup), plans.fetchMask(rowGroup));
        }

        /** A prefetcher that speculates one row group ahead under {@code budget}. */
        RowGroupPrefetcher prefetcherOver(FetchBudget budget) {
            ExecutorService fetchExecutor = Executors.newVirtualThreadPerTaskExecutor();
            return new RowGroupPrefetcher(
                    survivors, plans, fetcher, budget, fetchExecutor, /*prefetchDepth*/ 1, /*maxConcurrent*/ 1);
        }
    }

    private static RowMask maskOverFirstAndLastRows(RowGroupChunks chunks) {
        Map<ColumnPath, OffsetIndex> offsetIndexes = new HashMap<>();
        offsetIndexes.put(A, chunks.offsetIndex(A).orElseThrow());
        offsetIndexes.put(B, chunks.offsetIndex(B).orElseThrow());
        RowRanges firstAndLast =
                new RowRanges(List.of(new Range(0, 9), new Range(ROWS_PER_ROW_GROUP - 10, ROWS_PER_ROW_GROUP - 1)));
        return new RowMask(firstAndLast, offsetIndexes);
    }

    // -------------------------------------------------------------------------
    // File writing
    // -------------------------------------------------------------------------

    /**
     * Writes two row groups of {@value #ROWS_PER_ROW_GROUP} rows over two long columns, {@value #PAGE_VALUE_LIMIT} rows
     * per page, one batch per row group. Both columns are pinned to {@code PLAIN}, which leaves no dictionary page
     * ahead of a chunk and keeps every planned unit a run of data pages.
     */
    private static Path writeTwoRowGroupsOfTenPages() throws IOException {
        Path target = tempDir.resolve("fetch-span-reservation.parquet");
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .pageValueLimit(PAGE_VALUE_LIMIT)
                .defaultCompression(Compression.uncompressed())
                .rowGroupSize(RowGroupSize.rows(ROWS_PER_ROW_GROUP))
                .encodingPolicy(A.dot(), EncodingPolicy.FORCE_PLAIN)
                .encodingPolicy(B.dot(), EncodingPolicy.FORCE_PLAIN)
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(target), SCHEMA, options)) {
            writer.writeBatch(WriteFixtures.batch(SCHEMA, rowsOfRowGroup(0)));
            writer.writeBatch(WriteFixtures.batch(SCHEMA, rowsOfRowGroup(1)));
        }
        return target;
    }

    private static List<Map<ColumnPath, Object>> rowsOfRowGroup(int rowGroup) {
        List<Map<ColumnPath, Object>> rows = new ArrayList<>(ROWS_PER_ROW_GROUP);
        for (int i = 0; i < ROWS_PER_ROW_GROUP; i++) {
            long value = rowGroup * (long) ROWS_PER_ROW_GROUP + i;
            Map<ColumnPath, Object> row = new HashMap<>();
            row.put(A, Long.valueOf(value));
            row.put(B, Long.valueOf(value * 3L));
            rows.add(row);
        }
        return rows;
    }

    private static ParquetSchema twoLongColumnsSchema() {
        List<SchemaNode> leaves = List.of(requiredLong(A.name()), requiredLong(B.name()));
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, leaves, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredLong(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
    }
}
