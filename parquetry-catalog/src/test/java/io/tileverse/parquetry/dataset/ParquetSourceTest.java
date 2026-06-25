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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetRuntime;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.RowGroupSummary;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.RowGroupOutcome;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * End-to-end coverage for the {@code ParquetSource} facade: footer caching, filter / projection wiring, the
 * cross-thread reuse contract, and the single-permit sealing arrangement. Fixtures are written via {@code parquet-avro}
 * (same family as {@code EndToEndV2ReadTest}); the read path exercises the production page-cursor stack end-to-end.
 */
class ParquetSourceTest {

    private static final ColumnPath YEAR = ColumnPath.of("year");
    private static final ColumnPath COUNTRY = ColumnPath.of("country");
    private static final ColumnPath VALUE = ColumnPath.of("value");

    @Test
    void readsEveryRecordEndToEnd(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("dataset-end-to-end.parquet");
        List<RowDto> expected = generateRows(120);
        writeParquetFile(file, expected, CompressionCodecName.SNAPPY, 16L * 1024 * 1024);

        SegmentPool pool = SegmentPool.create();
        try (ByteRangeSource bytes = ByteRangeSource.ofFile(file)) {
            ParquetSource source = openWithPool(bytes, pool);
            ReadOptions options = ReadOptions.DEFAULTS;

            List<RowDto> actual = readAll(source, Predicate.ALWAYS_TRUE, Projection.ALL, options);

            assertThat(actual).hasSize(expected.size());
            assertRowsMatch(actual, expected);
        }
        assertThat(pool.stats().outstandingBorrows())
                .as("pooled buffers must drain after stream close")
                .isZero();
    }

    @Test
    void uncompressedRoundTrip(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("dataset-uncompressed.parquet");
        List<RowDto> expected = generateRows(50);
        writeParquetFile(file, expected, CompressionCodecName.UNCOMPRESSED, 16L * 1024 * 1024);

        SegmentPool pool = SegmentPool.create();
        try (ByteRangeSource bytes = ByteRangeSource.ofFile(file)) {
            ParquetSource source = openWithPool(bytes, pool);
            ReadOptions options = ReadOptions.DEFAULTS;

            try (Stream<ParquetRecord> records = source.read(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
                List<RowDto> actual = records.map(ParquetSourceTest::asRowDto).toList();
                assertThat(actual).hasSize(expected.size());
                assertRowsMatch(actual, expected);
            }
        }
        assertThat(pool.stats().outstandingBorrows()).isZero();
    }

    @Test
    void footerAndSchemaAreCachedAcrossReads(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("dataset-cache.parquet");
        List<RowDto> expected = generateRows(30);
        writeParquetFile(file, expected, CompressionCodecName.SNAPPY, 16L * 1024 * 1024);

        SegmentPool pool = SegmentPool.create();
        try (ByteRangeSource bytes = ByteRangeSource.ofFile(file)) {
            ParquetSource source = openWithPool(bytes, pool);

            // Schema, row-group view, and key/value metadata are stable across calls.
            assertThat(source.schema()).isSameAs(source.schema());
            assertThat(source.rowGroups()).isSameAs(source.rowGroups());
            assertThat(source.keyValueMetadata()).isNotNull();

            ReadOptions options = ReadOptions.DEFAULTS;
            for (int i = 0; i < 2; i++) {
                List<RowDto> actual = readAll(source, Predicate.ALWAYS_TRUE, Projection.ALL, options);
                assertThat(actual).hasSize(expected.size());
            }
        }
        assertThat(pool.stats().outstandingBorrows()).isZero();
    }

    @Test
    void rowGroupViewExposesIndexAndSizes(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("dataset-rgs.parquet");
        List<RowDto> expected = generateRows(2_000);
        // Tiny row-group target so parquet-avro emits multiple row groups.
        writeParquetFile(file, expected, CompressionCodecName.SNAPPY, 8_192L);

        try (ByteRangeSource bytes = ByteRangeSource.ofFile(file)) {
            ParquetSource source = ParquetSource.open(bytes);

            List<RowGroupSummary> view = source.rowGroups();
            assertThat(view).hasSizeGreaterThan(1);
            long totalRows = view.stream().mapToLong(RowGroupSummary::rowCount).sum();
            assertThat(totalRows).isEqualTo(expected.size());
            for (int i = 0; i < view.size(); i++) {
                assertThat(view.get(i).index()).isEqualTo(i);
                assertThat(view.get(i).rowCount()).isPositive();
                assertThat(view.get(i).totalByteSize()).isPositive();
            }
        }
    }

    @Test
    void predicateThatEliminatesEveryRowGroupReturnsNoRecords(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("dataset-filter-elim.parquet");
        List<RowDto> rows = generateRows(200);
        writeParquetFile(file, rows, CompressionCodecName.SNAPPY, 8_192L);

        SegmentPool pool = SegmentPool.create();
        try (ByteRangeSource bytes = ByteRangeSource.ofFile(file)) {
            ParquetSource source = openWithPool(bytes, pool);
            ReadOptions options = ReadOptions.DEFAULTS;
            Predicate impossibleYear = Pred.col("year").eq(9999);

            List<RowDto> matched = readAll(source, impossibleYear, Projection.ALL, options);
            assertThat(matched).isEmpty();

            ExplainPlan plan = source.explain(impossibleYear, Projection.ALL, options);
            assertThat(plan.rowGroups()).isNotEmpty();
            assertThat(plan.rowGroups()).allMatch(rg -> rg.outcome() == RowGroupOutcome.ELIMINATED);
        }
        assertThat(pool.stats().outstandingBorrows()).isZero();
    }

    @Test
    void predicateEliminatesRowGroupsWhoseStatsExclude(@TempDir Path tmp) throws Exception {
        // Year-clustered fixture: rows are written in contiguous blocks per year, giving most row groups a tight
        // [min, max] over `year`. The stats tier eliminates row groups whose year range excludes the predicate, and
        // record-level filtering then drops the non-matching rows inside surviving row groups. The result is exactly
        // the 2022 rows, and explain still shows a mix of eliminated and surviving row groups.
        Path file = tmp.resolve("dataset-filter-narrow.parquet");
        List<RowDto> rows = generateYearClusteredRows(2_000);
        writeParquetFile(file, rows, CompressionCodecName.SNAPPY, 8_192L);

        SegmentPool pool = SegmentPool.create();
        try (ByteRangeSource bytes = ByteRangeSource.ofFile(file)) {
            ParquetSource source = openWithPool(bytes, pool);
            ReadOptions options = ReadOptions.DEFAULTS;
            Predicate keepYear2022 = Pred.col("year").eq(2022);

            List<RowDto> matched = readAll(source, keepYear2022, Projection.ALL, options);
            List<RowDto> expectedYearRows =
                    rows.stream().filter(r -> r.year() == 2022).toList();
            assertThat(matched)
                    .as("record-level filtering returns exactly the matching rows")
                    .containsExactlyInAnyOrderElementsOf(expectedYearRows);

            ExplainPlan plan = source.explain(keepYear2022, Projection.ALL, options);
            assertThat(plan.rowGroups()).anyMatch(rg -> rg.outcome() == RowGroupOutcome.ELIMINATED);
            assertThat(plan.rowGroups()).anyMatch(rg -> rg.outcome() != RowGroupOutcome.ELIMINATED);
        }
        assertThat(pool.stats().outstandingBorrows()).isZero();
    }

    @Test
    void projectionDropsUnselectedColumns(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("dataset-proj.parquet");
        List<RowDto> rows = generateRows(80);
        writeParquetFile(file, rows, CompressionCodecName.SNAPPY, 16L * 1024 * 1024);

        SegmentPool pool = SegmentPool.create();
        try (ByteRangeSource bytes = ByteRangeSource.ofFile(file)) {
            ParquetSource source = openWithPool(bytes, pool);
            ReadOptions options = ReadOptions.DEFAULTS;
            Projection yearOnly = Projection.of(Set.of(YEAR));

            try (Stream<ParquetRecord> records = source.read(Predicate.ALWAYS_TRUE, yearOnly, options)) {
                List<ParquetRecord> collected = records.toList();
                assertThat(collected).hasSize(rows.size());
                for (int i = 0; i < rows.size(); i++) {
                    ParquetRecord rec = collected.get(i);
                    assertThat(rec.getInt(YEAR)).isEqualTo(rows.get(i).year());
                }
            }
        }
        assertThat(pool.stats().outstandingBorrows()).isZero();
    }

    @Test
    void concurrentReadsOnSharedDatasetSeeIdenticalContent(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("dataset-threads.parquet");
        List<RowDto> expected = generateRows(400);
        writeParquetFile(file, expected, CompressionCodecName.SNAPPY, 8_192L);

        SegmentPool pool = SegmentPool.create();
        try (ByteRangeSource bytes = ByteRangeSource.ofFile(file)) {
            ParquetSource source = openWithPool(bytes, pool);
            ReadOptions options = ReadOptions.DEFAULTS;

            int workers = 4;
            CountDownLatch ready = new CountDownLatch(workers);
            CountDownLatch go = new CountDownLatch(1);
            List<Thread> threads = new ArrayList<>(workers);
            List<AtomicReference<List<RowDto>>> results = new ArrayList<>(workers);
            List<AtomicReference<Throwable>> errors = new ArrayList<>(workers);
            for (int i = 0; i < workers; i++) {
                AtomicReference<List<RowDto>> slot = new AtomicReference<>();
                AtomicReference<Throwable> err = new AtomicReference<>();
                results.add(slot);
                errors.add(err);
                threads.add(Thread.ofVirtual().start(() -> {
                    try {
                        ready.countDown();
                        go.await();
                        slot.set(readAll(source, Predicate.ALWAYS_TRUE, Projection.ALL, options));
                    } catch (Throwable t) {
                        err.set(t);
                    }
                }));
            }
            ready.await();
            go.countDown();
            for (Thread t : threads) {
                t.join();
            }
            for (int i = 0; i < workers; i++) {
                assertThat(errors.get(i).get()).as("worker %d failed", i).isNull();
                List<RowDto> rows = results.get(i).get();
                assertThat(rows).as("worker %d row count", i).hasSize(expected.size());
                assertRowsMatch(rows, expected);
            }
        }
        assertThat(pool.stats().outstandingBorrows())
                .as("every concurrent read drained its pooled buffers")
                .isZero();
    }

    @Test
    void parquetSourceSealsToDefaultParquetSource() {
        // The ParquetSource facade is sealed and permits exactly DefaultParquetSource, the 1..N-files-same-schema
        // implementation. ParquetFileReader is the single-file read entry and is no longer a ParquetSource
        // implementation.
        Class<?>[] permitted = ParquetSource.class.getPermittedSubclasses();
        assertThat(permitted)
                .as("ParquetSource must seal to DefaultParquetSource")
                .containsExactly(DefaultParquetSource.class);
    }

    // --- read helpers ---

    /** Opens a dataset whose reads borrow buffers from {@code pool}; the test then asserts the pool drains. */
    private static ParquetSource openWithPool(ByteRangeSource source, SegmentPool pool) {
        OpenOptions openOptions = OpenOptions.builder()
                .runtime(ParquetRuntime.builder().segmentPool(pool).build())
                .build();
        return ParquetSource.open(source, openOptions);
    }

    // --- fixture helpers ---

    private static List<RowDto> generateRows(int count) {
        List<RowDto> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            rows.add(new RowDto(2020 + (i % 5), (i % 2 == 0) ? "AR" : "BR", i * 1.5));
        }
        return rows;
    }

    /**
     * Rows arranged in contiguous year blocks so each row group's {@code year} statistic has a tight, single-valued
     * range. Years cycle through {2020, 2021, 2022, 2023} in equal-sized blocks.
     */
    private static List<RowDto> generateYearClusteredRows(int count) {
        int years = 4;
        int blockSize = Math.max(1, count / years);
        List<RowDto> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int year = 2020 + Math.min(years - 1, i / blockSize);
            rows.add(new RowDto(year, (i % 2 == 0) ? "AR" : "BR", i * 1.5));
        }
        return rows;
    }

    private static void writeParquetFile(
            Path out, List<RowDto> rows, CompressionCodecName compression, long rowGroupBytes) throws IOException {
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse("""
                {"type":"record","name":"Row","fields":[
                  {"name":"year","type":"int"},
                  {"name":"country","type":"string"},
                  {"name":"value","type":"double"}
                ]}""");
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(
                        new LocalOutputFile(out))
                .withSchema(schema)
                .withCompressionCodec(compression)
                .withWriterVersion(WriterVersion.PARQUET_2_0)
                .withRowGroupSize(rowGroupBytes)
                .build()) {
            for (RowDto r : rows) {
                GenericData.Record parquetRecord = new GenericData.Record(schema);
                parquetRecord.put("year", r.year());
                parquetRecord.put("country", r.country());
                parquetRecord.put("value", r.value());
                writer.write(parquetRecord);
            }
        }
    }

    private static List<RowDto> readAll(
            ParquetSource source, Predicate predicate, Projection projection, ReadOptions opts) {
        try (Stream<ParquetRecord> records = source.read(predicate, projection, opts)) {
            return records.map(ParquetSourceTest::asRowDto).toList();
        }
    }

    private static RowDto asRowDto(ParquetRecord rec) {
        return new RowDto(rec.getInt(YEAR), rec.getString(COUNTRY), rec.getDouble(VALUE));
    }

    private static void assertRowsMatch(List<RowDto> actual, List<RowDto> expected) {
        assertThat(actual).hasSameSizeAs(expected);
        for (int i = 0; i < expected.size(); i++) {
            RowDto a = actual.get(i);
            RowDto e = expected.get(i);
            assertThat(a.year()).as("year @ row %d", i).isEqualTo(e.year());
            assertThat(a.country()).as("country @ row %d", i).isEqualTo(e.country());
            assertThat(a.value()).as("value @ row %d", i).isEqualTo(e.value());
        }
    }

    private record RowDto(int year, String country, double value) {}
}
