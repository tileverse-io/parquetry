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
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.RowRanges.Range;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.internal.filter.PageIndex;
import io.tileverse.parquetry.internal.footer.ChunkMeta;
import io.tileverse.parquetry.internal.read.PageIndexLearner.WholeChunk;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.DiskBudget;
import io.tileverse.parquetry.runtime.FetchBudget;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaBuilder;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

/**
 * Read-through behavioral test for the mandatory row-group fetch routing through the {@link FetchBufferAllocator}
 * valve. Constructing a {@link RowGroupFetcher} in isolation needs a footer-derived {@code RowGroupChunks}, a
 * {@code FetchPlan}, and an {@code IndexSectionLoader}, which only the read pipeline assembles. A read against a real
 * file with a tiny {@link FetchBudget} exercises the exact wiring this test guards: the prefetcher's inline path calls
 * {@code RowGroupFetcher.fetch}, which now allocates one buffer per row group through the valve. The test pins a
 * tiny-RAM/ample-disk runtime and asserts both that the mandatory fetch spilled to a disk mapping and that the rows
 * read are byte-identical to a default RAM runtime.
 *
 * <p>To check the learner, the tests fetch one row group of two long columns in pages of {@value #PAGE_VALUES} values.
 * A fetch planned without a mask hands its whole chunks to the learner of the read, and a fetch through a mask hands
 * nothing.
 */
class RowGroupFetcherTest {

    private static final Path FILE = CorpusFixtures.parquetTestingData().resolve("alltypes_plain.parquet");

    private static final ColumnPath A = ColumnPath.of("a");
    private static final ColumnPath B = ColumnPath.of("b");
    private static final int PAGED_ROWS = 300;
    private static final int PAGE_VALUES = 100;

    @TempDir
    Path tempDir;

    @Test
    @Timeout(30)
    void mandatoryFetchSpillsToDiskWhenRamIsExhaustedAndReadsCorrectly() {
        long ampleDisk = 256L << 20;
        ParquetRuntime ramRuntime = ParquetRuntime.builder().build();
        DiskBudget spillDisk = DiskBudget.ofBytes(ampleDisk);
        ParquetRuntime spillingRuntime = ParquetRuntime.builder()
                // below the bytes requested by any row group's plan, forcing the mandatory fetch to spill
                .fetchBudget(FetchBudget.ofBytes(1))
                .diskBudget(spillDisk)
                .build();

        List<Map<ColumnPath, Object>> baseline = readAllRows(ramRuntime);
        List<Map<ColumnPath, Object>> spilled = readAllRows(spillingRuntime);

        assertThat(spilled).as("rows read via the spilled mandatory fetch").isEqualTo(baseline);
        // The spill file is unmapped (and its disk reservation returned) as soon as the row group is decoded, which
        // can happen before any row is pulled. Assert against the low-water mark, which records the reservation even
        // after it has been released, rather than the live available bytes, which race the release.
        assertThat(spillDisk.minAvailable())
                .as("the mandatory fetch reserved disk for a spill mapping")
                .isLessThan(spillDisk.capacity());
    }

    /** Reads every row into a list of column-to-value maps. */
    private static List<Map<ColumnPath, Object>> readAllRows(ParquetRuntime runtime) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(FILE)) {
            ParquetFileReader dataset = ParquetFileReader.open(source, runtime, Optional.empty());
            List<ColumnPath> leaves = dataset.schema().leafColumns();
            try (Stream<ParquetRecord> rows =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                return collectRows(rows, leaves);
            }
        }
    }

    private static List<Map<ColumnPath, Object>> collectRows(Stream<ParquetRecord> rows, List<ColumnPath> leaves) {
        List<Map<ColumnPath, Object>> collected = new ArrayList<>();
        for (ParquetRecord row : (Iterable<ParquetRecord>) rows::iterator) {
            collected.add(snapshot(row, leaves));
        }
        return collected;
    }

    /** Materializes every leaf value for one row, copying binary leaves to bytes so equality compares by content. */
    private static Map<ColumnPath, Object> snapshot(ParquetRecord row, List<ColumnPath> leaves) {
        Map<ColumnPath, Object> values = new LinkedHashMap<>();
        for (ColumnPath leaf : leaves) {
            values.put(leaf, valueOf(row, leaf));
        }
        return values;
    }

    private static Object valueOf(ParquetRecord row, ColumnPath leaf) {
        if (row.isNull(leaf)) {
            return null;
        }
        Object raw = row.get(leaf);
        if (raw instanceof MemorySegment segment) {
            return Arrays.toString(segment.toArray(ValueLayout.JAVA_BYTE));
        }
        return raw;
    }

    // a fetch leaves the dictionary pages of a row group undecoded, and its buffer is the only borrow
    @Test
    void fetchDecodesNoDictionary() {
        Path file = CorpusFixtures.parquetTestingData().resolve("alltypes_dictionary.parquet");
        SegmentPool pool = SegmentPool.create();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            ParquetSchema schema = SchemaBuilder.build(footer.schema());
            RowGroupSurvivor survivor =
                    RowGroupSurvivor.full(TestRowGroupChunks.of(footer, 0, schema, TestRowGroupChunks.reading(source)));
            RowGroupFetcher fetcher = TestFetchers.over(source, schema, schema, pool);
            FetchPlan plan = fetcher.planFor(survivor, Optional.empty());
            try (RowGroupFetch fetch = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                long dictionaryColumns = fetch.columns().stream()
                        .filter(column -> column.dictionaryPage().isPresent())
                        .count();
                assertThat(dictionaryColumns)
                        .as("the fixture is dictionary-encoded")
                        .isPositive();
                assertThat(pool.stats().totalBorrows())
                        .as("one borrow for the fetch buffer")
                        .isEqualTo(1L);
            }
            assertThat(pool.stats().outstandingBorrows()).isZero();
        } finally {
            pool.close();
        }
    }

    @Test
    // S7466: the project style declares explicit local types rather than var
    @SuppressWarnings("java:S7466")
    void wholeFetchHandsChunksOver() throws IOException {
        RecordingLearner learner = new RecordingLearner(0);
        try (ByteRangeSource source = ByteRangeSource.ofFile(writePagedColumns())) {
            RowGroupSurvivor survivor = pagedSurvivor(source);
            RowGroupFetcher fetcher = TestFetchers.learning(source, pagedSchema(), SegmentPool.getDefault(), learner);
            FetchPlan plan = fetcher.planFor(survivor, Optional.empty());

            try (RowGroupFetch _ = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                assertThat(plan.unmasked()).isTrue();
            }

            assertThat(learner.calls()).isEqualTo(1);
            assertThat(learner.chunks()).extracting(WholeChunk::path).containsExactlyInAnyOrder(A, B);
            for (WholeChunk chunk : learner.chunks()) {
                String column = chunk.path().dot();
                assertThat(chunk.bytes().toArray(ValueLayout.JAVA_BYTE))
                        .as("the bytes of column %s from its first byte", column)
                        .isEqualTo(bytesOf(source, chunk.meta()));
            }
        }
    }

    @Test
    // S7466: the project style declares explicit local types rather than var
    @SuppressWarnings("java:S7466")
    void maskedFetchLearnsNothing() throws IOException {
        RecordingLearner learner = new RecordingLearner(0);
        try (ByteRangeSource source = ByteRangeSource.ofFile(writePagedColumns())) {
            RowGroupSurvivor survivor = pagedSurvivor(source);
            RowGroupFetcher fetcher = TestFetchers.learning(source, pagedSchema(), SegmentPool.getDefault(), learner);
            FetchPlan plan = fetcher.planFor(survivor, Optional.of(firstPageOf(survivor)));

            try (RowGroupFetch _ = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                assertThat(plan.unmasked()).isFalse();
            }

            assertThat(learner.calls()).isZero();
        }
    }

    @Test
    // S7466: the project style declares explicit local types rather than var
    @SuppressWarnings("java:S7466")
    void refusedGroupIsNotHandedOver() throws IOException {
        RecordingLearner learner = new RecordingLearner(1);
        try (ByteRangeSource source = ByteRangeSource.ofFile(writePagedColumns())) {
            RowGroupSurvivor survivor = pagedSurvivor(source);
            RowGroupFetcher fetcher = TestFetchers.learning(source, pagedSchema(), SegmentPool.getDefault(), learner);
            FetchPlan plan = fetcher.planFor(survivor, Optional.empty());

            try (RowGroupFetch _ = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                assertThat(plan.unmasked()).isTrue();
            }

            assertThat(learner.calls()).isZero();
        }
    }

    /** One row group of {@value #PAGED_ROWS} rows over two long columns, in pages of {@value #PAGE_VALUES} values. */
    private Path writePagedColumns() throws IOException {
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .pageValueLimit(PAGE_VALUES)
                .build();
        List<Map<ColumnPath, Object>> rows = new ArrayList<>(PAGED_ROWS);
        for (long row = 0; row < PAGED_ROWS; row++) {
            rows.add(Map.of(A, row, B, 2 * row));
        }
        return WriteFixtures.writeRows(tempDir.resolve("paged.parquet"), pagedSchema(), options, rows);
    }

    private static ParquetSchema pagedSchema() {
        return WriteFixtures.schemaOf(
                WriteFixtures.requiredLeaf("a", PrimitiveKind.INT64),
                WriteFixtures.requiredLeaf("b", PrimitiveKind.INT64));
    }

    private static RowGroupSurvivor pagedSurvivor(ByteRangeSource source) {
        FileMetaData footer = ParquetFormat.readFooter(source);
        ParquetSchema schema = SchemaBuilder.build(footer.schema());
        return RowGroupSurvivor.full(TestRowGroupChunks.of(footer, 0, schema, TestRowGroupChunks.reading(source)));
    }

    /** A mask keeping the rows of the first page of both columns. */
    private static RowMask firstPageOf(RowGroupSurvivor survivor) {
        RowGroupChunks chunks = survivor.chunks();
        PageIndex pagesOfA = chunks.pageLocations(A).orElseThrow();
        PageIndex pagesOfB = chunks.pageLocations(B).orElseThrow();
        RowRanges firstPage = new RowRanges(List.of(new Range(0, PAGE_VALUES - 1L)));
        return new RowMask(firstPage, Map.of(A, pagesOfA, B, pagesOfB));
    }

    private static byte[] bytesOf(ByteRangeSource source, ChunkMeta meta) {
        MemorySegment bytes = MemorySegment.ofArray(new byte[meta.totalCompressedSize()]);
        source.readFully(meta.chunkStart(), bytes);
        return bytes.toArray(ValueLayout.JAVA_BYTE);
    }

    /** A learner allowed one row group. It keeps a copy of each chunk handed to it, made while the fetch holds it. */
    private static final class RecordingLearner implements PageIndexLearner {

        private final int allowedRowGroup;
        private final List<WholeChunk> chunks = new ArrayList<>();
        private int calls;

        RecordingLearner(int allowedRowGroup) {
            this.allowedRowGroup = allowedRowGroup;
        }

        @Override
        public boolean learns(int rowGroup) {
            return rowGroup == allowedRowGroup;
        }

        @Override
        public void learn(RowGroupSurvivor survivor, List<WholeChunk> handed) {
            calls++;
            for (WholeChunk chunk : handed) {
                MemorySegment copy = MemorySegment.ofArray(chunk.bytes().toArray(ValueLayout.JAVA_BYTE));
                chunks.add(new WholeChunk(chunk.path(), chunk.meta(), copy));
            }
        }

        int calls() {
            return calls;
        }

        List<WholeChunk> chunks() {
            return chunks;
        }
    }
}
