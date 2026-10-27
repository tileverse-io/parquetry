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

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.internal.read.page.Dictionary;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.DiskBudget;
import io.tileverse.parquetry.runtime.FetchBudget;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
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
 */
class RowGroupFetcherTest {

    private static final Path FILE = CorpusFixtures.parquetTestingData().resolve("alltypes_plain.parquet");

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

    // the dictionary pages of a row group are decompressed into pooled buffers, never into a mapping of their own
    @Test
    void dictionaryPagesComeFromThePool() {
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
                int dictionaryColumns = (int) fetch.columns().stream()
                        .filter(column -> column.dictionary().isPresent())
                        .count();
                assertThat(dictionaryColumns)
                        .as("the fixture is dictionary-encoded")
                        .isPositive();
                assertThat(pool.stats().totalBorrows())
                        .as("one borrow for the fetch buffer and one per dictionary page")
                        .isEqualTo(1L + dictionaryColumns);
            }
            assertThat(pool.stats().outstandingBorrows()).isZero();
        } finally {
            pool.close();
        }
    }

    // the decoded dictionary holds heap copies: reusing the pool's buffers cannot change its values
    @Test
    void dictionaryOutlivesItsBuffer() {
        Path file = CorpusFixtures.parquetTestingData().resolve("alltypes_dictionary.parquet");
        SegmentPool pool = SegmentPool.create();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            ParquetSchema schema = SchemaBuilder.build(footer.schema());
            RowGroupSurvivor survivor =
                    RowGroupSurvivor.full(TestRowGroupChunks.of(footer, 0, schema, TestRowGroupChunks.reading(source)));
            RowGroupFetcher fetcher = TestFetchers.over(source, schema, schema, pool);
            FetchPlan plan = fetcher.planFor(survivor, Optional.empty());
            int stringColumn = schema.leafColumns().indexOf(ColumnPath.of("string_col"));
            Dictionary<?> strings;
            try (RowGroupFetch fetch = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                strings = fetch.columns().get(stringColumn).dictionary().orElseThrow();
            }
            byte[] first = ((MemorySegment) strings.get(0)).toArray(ValueLayout.JAVA_BYTE);
            assertThat(first).isNotEmpty();
            scribbleOverFreeBuffers(pool);
            assertThat(((MemorySegment) strings.get(0)).toArray(ValueLayout.JAVA_BYTE))
                    .as("the dictionary does not read from the returned buffer")
                    .isEqualTo(first);
        } finally {
            pool.close();
        }
    }

    /** Borrows every free buffer of the pool, overwrites it and returns it. */
    private static void scribbleOverFreeBuffers(SegmentPool pool) {
        int free = pool.stats().freeSegments();
        assertThat(free).as("the dictionary buffers went back to the pool").isPositive();
        List<SegmentPool.Pooled> borrowed = new ArrayList<>();
        for (int i = 0; i < free; i++) {
            SegmentPool.Pooled buffer = pool.borrow(8192);
            buffer.segment().fill((byte) 0xFF);
            borrowed.add(buffer);
        }
        borrowed.forEach(SegmentPool.Pooled::close);
    }
}
