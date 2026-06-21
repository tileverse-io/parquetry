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
package io.tileverse.parquetry.data;

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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
import io.tileverse.parquetry.filter.explain.Tier;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.observe.QueryObserver;
import io.tileverse.parquetry.observe.QueryStats;
import io.tileverse.parquetry.observe.RowGroupRead;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Verifies that {@code onRowGroupRead} fires once per non-eliminated row group during decode, for the {@code read},
 * {@code count}, and {@code readBatches} paths, with correct {@code rowGroupIndex}, {@code rowsDecoded},
 * {@code rowsMatched}, and {@code pagesDecoded}.
 *
 * <p>The fixture writes ids {@code 0..8} four rows per row group, hence three row groups holding {@code [0,1,2,3]},
 * {@code [4,5,6,7]}, and {@code [8]}. The predicate {@code id >= 5} eliminates the first row group from its statistics,
 * proving the reported index is the true file ordinal (1 and 2) rather than the dense survivor position (0 and 1).
 */
class RowGroupReadFiringIT {

    private static final Predicate PREDICATE = col("id").gtEq(5);
    private static final long EXPECTED_MATCHES = 4L; // ids 5, 6, 7, 8

    @Test
    void readFiresOnePerNonEliminatedRowGroup(@TempDir Path tmp) throws Exception {
        Path file = writeThreeRowGroups(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetReader reader = ParquetReader.open(source);
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
            ParquetReader reader = ParquetReader.open(source);
            RecordingObserver observer = new RecordingObserver();
            ReadOptions options = optionsWith(observer);

            long matched = reader.count(PREDICATE, options);

            assertThat(matched).isEqualTo(EXPECTED_MATCHES);
            assertReadEvents(observer);
        }
    }

    @Test
    void readBatchesReportsEveryDecodedRowAsMatched(@TempDir Path tmp) throws Exception {
        Path file = writeThreeRowGroups(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetReader reader = ParquetReader.open(source);
            RecordingObserver observer = new RecordingObserver();
            ReadOptions options = optionsWith(observer);

            try (Stream<?> batches = reader.readBatches(PREDICATE, Projection.ALL, options)) {
                batches.forEach(batch -> {});
            }

            List<RowGroupRead> events = observer.events;
            assertThat(events).as("one event per non-eliminated row group").hasSize(2);
            assertThat(events)
                    .allSatisfy(event -> assertThat(event.rowsMatched()).isEqualTo(event.rowsDecoded()));
            assertThat(events).extracting(RowGroupRead::rowGroupIndex).containsExactlyInAnyOrder(1, 2);
            assertThat(events)
                    .allSatisfy(event -> assertThat(event.pagesDecoded()).isGreaterThanOrEqualTo(1));
        }
    }

    /**
     * Late materialization runs phase 1 (predicate column) in a separate reader from phase 2 (output column). When a
     * scanned row group matches no row, phase 2 never opens, yet phase 1 still decoded the predicate column's pages.
     * The emitted {@code pagesDecoded} must reflect that phase-1 decode, not drop to zero.
     *
     * <p>The fixture writes 90 rows in three 30-row groups, {@code key = 2*i} (only even keys), {@code payload = i}.
     * The predicate {@code key == 61} (an odd value, present in no row) falls inside row group 1's key range
     * {@code [60, 118]} but outside groups 0 and 2, hence statistics eliminate groups 0 and 2 and group 1 alone is
     * scanned. Phase 1 decodes group 1's {@code key} pages, matches nothing, and phase 2 never runs - exactly the case
     * the fix accounts for. The projection is {@code payload} only (the predicate column absent from the output), which
     * the late-mat planner requires; {@code pageValueLimit} forces several pages per column so an offset index is
     * written for each.
     */
    @Test
    void lateMaterializationCountsPhaseOnePredicatePages(@TempDir Path tmp) throws Exception {
        Path file = writeLateMaterializationFixture(tmp);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetReader reader = ParquetReader.open(source);
            RecordingObserver observer = new RecordingObserver();
            ReadOptions options = optionsWith(observer);
            Predicate keyIs61 = col("key").eq(61L);
            Projection payloadOnly = Projection.of(Set.of(ColumnPath.of("payload")));

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
            assertThat(scanned.pagesDecoded())
                    .as("phase-1 predicate-column pages are counted even though phase 2 never ran")
                    .isGreaterThanOrEqualTo(1);
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
            ParquetReader reader = ParquetReader.open(source);
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
            ParquetReader reader = ParquetReader.open(source);
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
            ParquetReader reader = ParquetReader.open(source);
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
            ParquetReader reader = ParquetReader.open(source);
            RecordingObserver observer = new RecordingObserver();
            ReadOptions options = optionsWith(observer);

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
            // readBatches reports every decoded row as matched (no record-level filter on the batch path).
            assertThat(stats.rowsMatched()).isEqualTo(stats.rowsDecoded());
            assertThat(stats.rowGroupsEliminatedByTier()).containsEntry(Tier.STATS, 1);
        }
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
                ParquetWriter writer = ParquetWriter.create(out, schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(1);
            for (int i = 0; i < 9; i++) {
                WriteFixtures.appendRow(appender, schema, Map.of(ColumnPath.of("id"), i));
            }
        }
        return file;
    }

    private static Path writeLateMaterializationFixture(Path tmp) throws Exception {
        ParquetSchema schema = flatSchema(requiredInt64("key"), requiredInt64("payload"));
        WriteOptions options = WriteOptions.builder()
                .tempDir(tmp)
                .rowGroupSize(RowGroupSize.rows(30L))
                .pageValueLimit(8)
                .build();
        Path file = tmp.resolve("late-mat.parquet");
        try (OutputStream out = Files.newOutputStream(file);
                ParquetWriter writer = ParquetWriter.create(out, schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(1);
            for (int i = 0; i < 90; i++) {
                WriteFixtures.appendRow(
                        appender, schema, Map.of(ColumnPath.of("key"), (long) (i * 2), ColumnPath.of("payload"), (long)
                                i));
            }
        }
        return file;
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

    private static final class RecordingObserver implements QueryObserver {

        private final List<RowGroupRead> events = new ArrayList<>();
        private int finishedCount;
        private QueryStats lastStats;

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
