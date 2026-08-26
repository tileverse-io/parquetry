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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.SpatialReadProbe;
import io.tileverse.parquetry.filter.SpatialReadProbe.Decision;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.RowGroupOutcome;
import io.tileverse.parquetry.filter.explain.RowGroupPlan;
import io.tileverse.parquetry.filter.explain.Tier;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.observe.QueryObserver;
import io.tileverse.parquetry.observe.QueryStats;
import io.tileverse.parquetry.observe.RowGroupRead;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.Wkb;

/**
 * Verifies that {@code onRowGroupRead} fires once per non-eliminated row group during decode, for the {@code read},
 * {@code count}, and {@code readBatches} paths, with correct {@code rowGroupIndex}, {@code rowsDecoded},
 * {@code rowsMatched}, {@code pagesDecoded}, and record-filter time.
 *
 * <p>The base fixture writes ids {@code 0..8} four rows per row group, hence three row groups holding
 * {@code [0,1,2,3]}, {@code [4,5,6,7]}, and {@code [8]}. The predicate {@code id >= 5} eliminates the first row group
 * from its statistics, proving the reported index is the true file ordinal (1 and 2) rather than the dense survivor
 * position (0 and 1).
 */
class RowGroupReadFiringIT {

    private static final Predicate PREDICATE = col("id").gtEq(5);
    private static final long EXPECTED_MATCHES = 4L; // ids 5, 6, 7, 8

    private static final int FILTERED_READ_ROWS = 120;
    private static final int FILTERED_READ_MATCHES = 10;
    private static final Predicate FILTERED_READ_PREDICATE =
            Pred.and(col("id").gtEq(10L), col("id").lt(20L));
    private static final Projection FILTERED_READ_OUTPUT =
            Projection.ofPhysical(List.of(ColumnPath.of("v"), ColumnPath.of("name")));

    private static final int POINT_CELLS = 3;
    private static final int POINTS_PER_CELL = 4;
    private static final int POINT_ROWS = POINT_CELLS * POINTS_PER_CELL;

    @Test
    void readFiresOnePerNonEliminatedRowGroup(@TempDir Path tmp) throws Exception {
        Path file = writeThreeRowGroups(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            RecordingObserver observer = new RecordingObserver();
            ReadOptions options = optionsWith(observer);

            try (Stream<?> rows = reader.read(PREDICATE, Projection.ALL, options)) {
                rows.forEach(row -> {});
            }

            assertReadEvents(observer);
        }
    }

    @Test
    void countFiresOnePerNonEliminatedRowGroup(@TempDir Path tmp) throws Exception {
        Path file = writeThreeRowGroups(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            RecordingObserver observer = new RecordingObserver();
            ReadOptions options = optionsWith(observer);

            long matched = reader.count(PREDICATE, options);

            assertThat(matched).isEqualTo(EXPECTED_MATCHES);
            assertReadEvents(observer);
        }
    }

    @Test
    void readBatchesReportsTheRowsItEmittedAsMatched(@TempDir Path tmp) throws Exception {
        Path file = writeThreeRowGroups(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            RecordingObserver observer = new RecordingObserver();
            ReadOptions options = optionsWith(observer);

            try (Stream<?> batches = reader.readBatches(PREDICATE, Projection.ALL, options)) {
                batches.forEach(batch -> {});
            }

            List<RowGroupRead> events = observer.events;
            assertThat(events).as("one event per non-eliminated row group").hasSize(2);
            assertThat(events).extracting(RowGroupRead::rowGroupIndex).containsExactlyInAnyOrder(1, 2);
            assertThat(events)
                    .allSatisfy(event -> assertThat(event.rowsMatched())
                            .as("a row group emits no more rows than it decoded")
                            .isLessThanOrEqualTo(event.rowsDecoded()));
            assertThat(events.stream().mapToLong(RowGroupRead::rowsMatched).sum())
                    .as("the emitted rows across row groups are the predicate's matches")
                    .isEqualTo(EXPECTED_MATCHES);
            assertThat(events)
                    .allSatisfy(event -> assertThat(event.pagesDecoded()).isGreaterThanOrEqualTo(1));
        }
    }

    /**
     * A masked scan runs the predicate over the filter column and materializes the output column only for the rows that
     * survive. When a scanned row group matches no row, no output value is materialized, yet the filter column's pages
     * were decoded and its rows evaluated: {@code rowsDecoded} reports those filter-column rows and
     * {@code pagesDecoded} does not drop to zero.
     *
     * <p>The fixture writes 90 rows in three 30-row groups, {@code key = 2*i} (only even keys), {@code payload = i}.
     * The predicate {@code key == 61} (an odd value, present in no row) falls inside row group 1's key range
     * {@code [60, 118]} but outside groups 0 and 2, hence statistics eliminate groups 0 and 2 and group 1 alone is
     * scanned. The column-index tier then narrows group 1 to the pages whose key range spans 61, and the scan walks
     * exactly those rows. {@code pageValueLimit} forces several pages per column so an offset index is written for
     * each.
     */
    @Test
    void maskedScanCountsPredicateColumnPages(@TempDir Path tmp) throws Exception {
        Path file = writeMaskedScanFixture(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            RecordingObserver observer = new RecordingObserver();
            ReadOptions options = optionsWith(observer);
            Predicate keyIs61 = col("key").eq(61L);
            Projection payloadOnly = Projection.ofPhysical(Set.of(ColumnPath.of("payload")));
            long scannedRows = survivingRowsOfRowGroup(reader, keyIs61, payloadOnly, 1);

            try (Stream<?> rows = reader.read(keyIs61, payloadOnly, options)) {
                rows.forEach(row -> {});
            }

            List<RowGroupRead> events = observer.events;
            assertThat(events)
                    .as("only row group 1 survives statistics; groups 0 and 2 are eliminated")
                    .hasSize(1);
            RowGroupRead scanned = events.get(0);
            assertThat(scanned.rowGroupIndex()).isEqualTo(1);
            assertThat(scanned.rowsMatched())
                    .as("the odd key matches no even-keyed row")
                    .isZero();
            assertThat(scanned.rowsDecoded())
                    .as("every filter-column row the scan walked is reported decoded")
                    .isEqualTo(scannedRows);
            assertThat(scanned.pagesDecoded())
                    .as("the predicate column's pages are counted even though no row survived to materialize")
                    .isGreaterThanOrEqualTo(1);
        }
    }

    /**
     * A column that both filters and outputs is read once. Adding the predicate column to the projection therefore
     * changes neither the pages decoded nor the rows decoded: the scan gathers the surviving rows out of the window it
     * already evaluated rather than opening a second reader over the same column.
     */
    @Test
    void maskedScanReadsAFilterAndOutputColumnOnce(@TempDir Path tmp) throws Exception {
        Path file = writeMaskedScanFixture(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            Predicate keyIs60 = col("key").eq(60L);
            ColumnPath key = ColumnPath.of("key");
            ColumnPath payload = ColumnPath.of("payload");

            RowGroupRead payloadOnly = scanRowGroup(reader, keyIs60, Projection.ofPhysical(Set.of(payload)));
            RowGroupRead keyAndPayload = scanRowGroup(reader, keyIs60, Projection.ofPhysical(Set.of(key, payload)));

            assertThat(keyAndPayload.pagesDecoded())
                    .as("projecting the predicate column adds no page decode")
                    .isEqualTo(payloadOnly.pagesDecoded());
            assertThat(keyAndPayload.rowsDecoded())
                    .as("projecting the predicate column adds no row decode")
                    .isEqualTo(payloadOnly.rowsDecoded());
            assertThat(keyAndPayload.rowsMatched())
                    .as("the one even key in range matches")
                    .isEqualTo(1L);
            assertThat(readLongColumn(reader, keyIs60, Projection.ofPhysical(Set.of(key, payload)), key))
                    .as("the gathered value of the shared column reaches the output row")
                    .containsExactly(60L);
        }
    }

    /**
     * A filtered read evaluates the predicate in scan and materializes only the rows that survive it. Its event reports
     * every filter-column row as decoded, the survivors as matched, fewer output-column pages than a full scan of those
     * columns decodes, and the scan's own predicate evaluation as record-filter time.
     */
    @Test
    void aMaskedScanReportsFilterRowsDecodedAndSurvivorsMatched(@TempDir Path tmp) throws Exception {
        Path file = writeFilteredReadFixture(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            RecordingObserver observer = new RecordingObserver(true);
            ReadOptions options = optionsWith(observer);
            int fullScanPages = pagesDecodedByFullScan(reader, FILTERED_READ_OUTPUT);

            try (Stream<ParquetRecord> rows = reader.read(FILTERED_READ_PREDICATE, FILTERED_READ_OUTPUT, options)) {
                assertThat(rows.count()).isEqualTo(FILTERED_READ_MATCHES);
            }

            assertThat(observer.events).hasSize(1);
            RowGroupRead event = observer.events.get(0);
            assertThat(event.rowsDecoded())
                    .as("the predicate column's rows are the decoded rows")
                    .isEqualTo(FILTERED_READ_ROWS);
            assertThat(event.rowsMatched()).as("only the survivors matched").isEqualTo(FILTERED_READ_MATCHES);
            assertThat(event.pagesDecoded())
                    .as("the output columns decode fewer pages than a full scan of them would")
                    .isLessThan(fullScanPages);
            assertThat(event.timings().orElseThrow().recordFilterNanos())
                    .as("the per-window predicate evaluation is attributed to the record filter")
                    .isPositive();
        }
    }

    /**
     * The batches path evaluates the same predicate in the same scan, and reports the same rows and the same
     * record-filter time: the scan's own evaluation reaches the event no matter which entry point drains it.
     */
    @Test
    void aMaskedScanAttributesItsPredicateTimeOnTheBatchesPath(@TempDir Path tmp) throws Exception {
        Path file = writeFilteredReadFixture(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            RecordingObserver observer = new RecordingObserver(true);
            ReadOptions options = optionsWith(observer);

            try (Stream<ParquetRecordBatch> batches =
                    reader.readBatches(FILTERED_READ_PREDICATE, FILTERED_READ_OUTPUT, options)) {
                batches.forEach(ParquetRecordBatch::close);
            }

            assertThat(observer.events).hasSize(1);
            RowGroupRead event = observer.events.get(0);
            assertThat(event.rowsMatched())
                    .as("the batches path emits the same survivors the row path does")
                    .isEqualTo(FILTERED_READ_MATCHES);
            assertThat(event.timings().orElseThrow().recordFilterNanos())
                    .as("the scan's predicate evaluation is attributed to the record filter on the batches path too")
                    .isPositive();
        }
    }

    /**
     * A decimating spatial probe drops rows the read would otherwise emit. The rows the read did emit are the rows it
     * reports matched: a probe that keeps one point per integer-X cell leaves fewer matched rows than decoded ones.
     */
    @Test
    void aDecimatingProbeReportsOnlyTheRowsItEmitted(@TempDir Path tmp) throws Exception {
        Path file = writePointsAcrossIntegerXCells(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            RecordingObserver observer = new RecordingObserver();
            ReadOptions options = ReadOptions.builder()
                    .queryObserver(observer)
                    .spatialReadProbe(keepFirstPointPerIntegerXCell())
                    .build();

            long emitted = emittedRows(reader, options);

            assertThat(emitted).as("one point survives per integer-X cell").isEqualTo(POINT_CELLS);
            assertThat(observer.events).hasSize(1);
            RowGroupRead event = observer.events.get(0);
            assertThat(event.rowsDecoded())
                    .as("every point was decoded for the probe to see it")
                    .isEqualTo(POINT_ROWS);
            assertThat(event.rowsMatched())
                    .as("the decimated rows are the rows the read emitted")
                    .isEqualTo(emitted);
        }
    }

    /**
     * An early-terminated stream must not report rows it never delivered. The coordinator decodes ahead speculatively;
     * closing the stream after one row leaves the prefetched row groups undelivered. Only the one delivered group may
     * fire a {@link RowGroupRead}, and its {@code rowsDecoded} is bounded by that group's actual row count (the
     * producer may have decoded ahead of consumption within the group, never beyond it).
     */
    @Test
    void earlyCloseFiresEventsOnlyForDeliveredRowGroups(@TempDir Path tmp) throws Exception {
        Path file = writeThreeRowGroups(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            RecordingObserver observer = new RecordingObserver();
            ReadOptions options = optionsWith(observer);

            try (Stream<?> rows = reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
                rows.limit(1).forEach(row -> {});
            }

            List<RowGroupRead> events = observer.events;
            assertThat(events)
                    .as("only the delivered row group fires; undelivered speculative groups stay silent")
                    .hasSize(1);
            RowGroupRead delivered = events.get(0);
            assertThat(delivered.rowGroupIndex()).isEqualTo(0);
            assertThat(delivered.rowsDecoded())
                    .as("rows decoded stay within the delivered group's row count")
                    .isBetween(1L, 4L);
            long decodedSum =
                    events.stream().mapToLong(RowGroupRead::rowsDecoded).sum();
            assertThat(decodedSum)
                    .as("the summed decoded rows stay below the table's total row count")
                    .isLessThan(9L);
        }
    }

    @Test
    void readDeliversAggregatedQueryStats(@TempDir Path tmp) throws Exception {
        Path file = writeThreeRowGroups(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            RecordingObserver observer = new RecordingObserver();
            ReadOptions options = optionsWith(observer);

            try (Stream<?> rows = reader.read(PREDICATE, Projection.ALL, options)) {
                rows.forEach(row -> {});
            }

            assertAggregatedStats(observer);
        }
    }

    @Test
    void countDeliversAggregatedQueryStats(@TempDir Path tmp) throws Exception {
        Path file = writeThreeRowGroups(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            RecordingObserver observer = new RecordingObserver();
            ReadOptions options = optionsWith(observer);

            reader.count(PREDICATE, options);

            assertAggregatedStats(observer);
        }
    }

    @Test
    void readBatchesDeliversAggregatedQueryStats(@TempDir Path tmp) throws Exception {
        Path file = writeThreeRowGroups(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            RecordingObserver observer = new RecordingObserver();
            ReadOptions options = optionsWith(observer);
            long scannedRows = scannedRowsOf(reader, PREDICATE, Projection.ALL);

            try (Stream<?> batches = reader.readBatches(PREDICATE, Projection.ALL, options)) {
                batches.forEach(batch -> {});
            }

            QueryStats stats = observer.lastStats;
            assertThat(observer.finishedCount)
                    .as("onQueryFinished fires exactly once")
                    .isEqualTo(1);
            assertThat(stats.rowGroupsTotal())
                    .as("three row groups in the file")
                    .isEqualTo(3);
            assertThat(stats.rowGroupsRead())
                    .as("two non-eliminated groups decoded")
                    .isEqualTo(2);
            assertThat(stats.rowsMatched())
                    .as("readBatches applies the predicate exactly and reports only the rows it emitted")
                    .isEqualTo(EXPECTED_MATCHES);
            assertThat(stats.rowsDecoded())
                    .as("every row the plan left for the two non-eliminated groups is reported decoded")
                    .isEqualTo(scannedRows);
            assertThat(stats.rowGroupsEliminatedByTier()).containsEntry(Tier.STATS, 1);
        }
    }

    /** Drains one read and returns the single row group's read event. */
    private static RowGroupRead scanRowGroup(ParquetFileReader reader, Predicate predicate, Projection projection) {
        RecordingObserver observer = new RecordingObserver();
        try (Stream<?> rows = reader.read(predicate, projection, optionsWith(observer))) {
            rows.forEach(row -> {});
        }
        assertThat(observer.events).hasSize(1);
        return observer.events.get(0);
    }

    /** The values of {@code column} in the rows one read delivers, taken before each flyweight row moves on. */
    private static List<Long> readLongColumn(
            ParquetFileReader reader, Predicate predicate, Projection projection, ColumnPath column) {
        try (Stream<ParquetRecord> rows = reader.read(predicate, projection, ReadOptions.DEFAULTS)) {
            return rows.map(row -> row.getLong(column)).toList();
        }
    }

    /** The rows the plan leaves for decode across every non-eliminated row group. */
    private static long scannedRowsOf(ParquetFileReader reader, Predicate predicate, Projection projection) {
        ExplainPlan plan = reader.explain(predicate, projection, ReadOptions.DEFAULTS);
        long total = 0L;
        for (RowGroupPlan rowGroup : plan.rowGroups()) {
            if (rowGroup.outcome() != RowGroupOutcome.ELIMINATED) {
                total += rowsToScan(rowGroup);
            }
        }
        return total;
    }

    /** The rows the filter pipeline left for row group {@code rowGroupIndex} to scan, from the plan alone. */
    private static long survivingRowsOfRowGroup(
            ParquetFileReader reader, Predicate predicate, Projection projection, int rowGroupIndex) {
        ExplainPlan plan = reader.explain(predicate, projection, ReadOptions.DEFAULTS);
        for (RowGroupPlan rowGroup : plan.rowGroups()) {
            if (rowGroup.index() == rowGroupIndex) {
                return rowsToScan(rowGroup);
            }
        }
        throw new IllegalStateException("Row group " + rowGroupIndex + " is not in the plan");
    }

    /** One row group's rows left for decode: those the plan narrowed it to, or every row when it narrowed none. */
    private static long rowsToScan(RowGroupPlan rowGroup) {
        return rowGroup.survivingRows().map(RowRanges::totalRows).orElseGet(rowGroup::rowCount);
    }

    private static void assertAggregatedStats(RecordingObserver observer) {
        QueryStats stats = observer.lastStats;
        assertThat(observer.finishedCount)
                .as("onQueryFinished fires exactly once")
                .isEqualTo(1);
        assertThat(stats.rowGroupsTotal()).as("three row groups in the file").isEqualTo(3);
        assertThat(stats.rowGroupsRead())
                .as("two non-eliminated groups decoded")
                .isEqualTo(2);
        assertThat(stats.rowsMatched()).as("actual match count for id >= 5").isEqualTo(EXPECTED_MATCHES);
        assertThat(stats.rowGroupsEliminatedByTier())
                .as("the first row group is eliminated by statistics")
                .containsEntry(Tier.STATS, 1);
    }

    private static void assertReadEvents(RecordingObserver observer) {
        List<RowGroupRead> events = observer.events;
        assertThat(events).as("one event per non-eliminated row group").hasSize(2);
        assertThat(events)
                .as("indices are the true file ordinals, not the dense survivor positions")
                .extracting(RowGroupRead::rowGroupIndex)
                .containsExactlyInAnyOrder(1, 2);

        long matchedSum = events.stream().mapToLong(RowGroupRead::rowsMatched).sum();
        assertThat(matchedSum).as("matched rows sum to the actual match count").isEqualTo(EXPECTED_MATCHES);

        long decodedSum = events.stream().mapToLong(RowGroupRead::rowsDecoded).sum();
        assertThat(decodedSum)
                .as("decoded rows cover at least the matched rows")
                .isGreaterThanOrEqualTo(matchedSum);

        RowGroupRead scanned = eventForRowGroup(events, 1);
        assertThat(scanned.pagesDecoded())
                .as("the scanned row group decoded at least one page")
                .isGreaterThanOrEqualTo(1);
    }

    private static RowGroupRead eventForRowGroup(List<RowGroupRead> events, int rowGroupIndex) {
        return events.stream()
                .filter(event -> event.rowGroupIndex() == rowGroupIndex)
                .findFirst()
                .orElseThrow();
    }

    private static Path writeThreeRowGroups(Path tmp) throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options = WriteOptions.builder()
                .tempDir(tmp)
                .rowGroupSize(RowGroupSize.rows(4L))
                .build();
        Path file = tmp.resolve("three-row-groups.parquet");
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(1);
            for (int i = 0; i < 9; i++) {
                WriteFixtures.appendRow(appender, schema, Map.of(ColumnPath.of("id"), i));
            }
        }
        return file;
    }

    private static Path writeMaskedScanFixture(Path tmp) throws Exception {
        ParquetSchema schema = flatSchema(requiredInt64("key"), requiredInt64("payload"));
        WriteOptions options = WriteOptions.builder()
                .tempDir(tmp)
                .rowGroupSize(RowGroupSize.rows(30L))
                .pageValueLimit(8)
                .build();
        Path file = tmp.resolve("masked-scan.parquet");
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(1);
            for (int i = 0; i < 90; i++) {
                WriteFixtures.appendRow(
                        appender, schema, Map.of(ColumnPath.of("key"), (long) (i * 2), ColumnPath.of("payload"), (long)
                                i));
            }
        }
        return file;
    }

    /**
     * 120 rows in one row group at eight values per page: {@code id} is the filter column, {@code v} and {@code name}
     * the output columns. The ten ids the query matches sit in the first ten rows, and every later row alternates a
     * value below the queried range with one above it, which leaves every page's id range overlapping the query and
     * hence unprunable. The scan therefore walks all 120 rows while its output columns touch only the two pages the
     * survivors fall in.
     */
    private static Path writeFilteredReadFixture(Path tmp) throws Exception {
        ParquetSchema schema = flatSchema(requiredInt64("id"), requiredInt64("v"), requiredString("name"));
        WriteOptions options = WriteOptions.builder()
                .tempDir(tmp)
                .rowGroupSize(RowGroupSize.rows(FILTERED_READ_ROWS))
                .pageValueLimit(8)
                .build();
        Path file = tmp.resolve("filtered-read.parquet");
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(1);
            for (int row = 0; row < FILTERED_READ_ROWS; row++) {
                WriteFixtures.appendRow(appender, schema, filteredReadRow(row));
            }
        }
        return file;
    }

    private static Map<ColumnPath, Object> filteredReadRow(int row) {
        return Map.of(
                ColumnPath.of("id"), idAt(row), ColumnPath.of("v"), (long) row, ColumnPath.of("name"), "name-" + row);
    }

    /** The ten matching ids in the first ten rows; every later row alternates below and above the queried range. */
    private static long idAt(int row) {
        if (row < FILTERED_READ_MATCHES) {
            return 10L + row;
        }
        if (row % 2 == 0) {
            return 9L;
        }
        return 20L + row;
    }

    /** Four points in each of three integer-X cells, in one row group, under a GeoParquet geometry column. */
    private static Path writePointsAcrossIntegerXCells(Path tmp) throws Exception {
        ParquetSchema schema = flatSchema(requiredBinary("geometry"));
        WriteOptions options =
                WriteOptions.builder().tempDir(tmp).crsEpsg("geometry", 4326).build();
        Path file = tmp.resolve("points.parquet");
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(POINT_ROWS);
            for (int cell = 0; cell < POINT_CELLS; cell++) {
                for (int withinCell = 0; withinCell < POINTS_PER_CELL; withinCell++) {
                    WriteFixtures.appendRow(appender, schema, pointRow(cell, withinCell));
                }
            }
            appender.flush();
        }
        return file;
    }

    private static Map<ColumnPath, Object> pointRow(int cell, int withinCell) {
        double x = cell + 0.1 * withinCell;
        double y = 10.0 + cell;
        return Map.of(ColumnPath.of("geometry"), Wkb.fromWkt("POINT (" + x + " " + y + ")"));
    }

    /** A probe that keeps the first point it sees in each integer-X cell and skips every later point in that cell. */
    private static SpatialReadProbe keepFirstPointPerIntegerXCell() {
        Set<Integer> painted = new HashSet<>();
        return (minX, _, _, _) -> painted.add((int) Math.floor(minX)) ? Decision.keep() : Decision.skip();
    }

    /** The data pages an unfiltered read of {@code projection} decodes for the fixture's single row group. */
    private static int pagesDecodedByFullScan(ParquetFileReader reader, Projection projection) {
        RecordingObserver observer = new RecordingObserver();
        try (Stream<ParquetRecord> rows = reader.read(Predicate.ALWAYS_TRUE, projection, optionsWith(observer))) {
            rows.forEach(row -> {});
        }
        assertThat(observer.events).hasSize(1);
        return observer.events.get(0).pagesDecoded();
    }

    /** The rows an unfiltered batch read emits under {@code options}, which a decimating probe narrows. */
    private static long emittedRows(ParquetFileReader reader, ReadOptions options) {
        try (Stream<ParquetRecordBatch> batches = reader.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
            return batches.mapToLong(RowGroupReadFiringIT::rowCountAndClose).sum();
        }
    }

    private static long rowCountAndClose(ParquetRecordBatch batch) {
        try (batch) {
            return batch.rowCount();
        }
    }

    private static ReadOptions optionsWith(QueryObserver observer) {
        return ReadOptions.builder().queryObserver(observer).build();
    }

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children = Stream.of(leaves).map(f -> (SchemaNode) f).toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredInt32(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive requiredInt64(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive requiredString(String name) {
        return new SchemaNode.Primitive(
                name,
                Repetition.REQUIRED,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
    }

    private static SchemaNode.Primitive requiredBinary(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static final class RecordingObserver implements QueryObserver {

        private final List<RowGroupRead> events = new ArrayList<>();
        private final boolean wantsTimings;
        private int finishedCount;
        private QueryStats lastStats;

        RecordingObserver() {
            this(false);
        }

        RecordingObserver(boolean wantsTimings) {
            this.wantsTimings = wantsTimings;
        }

        @Override
        public boolean wantsTimings() {
            return wantsTimings;
        }

        @Override
        public synchronized void onRowGroupRead(RowGroupRead event) {
            events.add(event);
        }

        @Override
        public synchronized void onQueryFinished(QueryStats stats) {
            finishedCount++;
            this.lastStats = stats;
        }
    }
}
