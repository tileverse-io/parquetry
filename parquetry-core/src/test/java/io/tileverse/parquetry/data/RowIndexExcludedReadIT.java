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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.SortedLongPositionSet;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.explain.RowGroupOutcome;
import io.tileverse.parquetry.filter.explain.RowGroupPlan;
import io.tileverse.parquetry.filter.explain.Tier;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.internal.read.TestParquetFiles;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * End-to-end check that a {@link Predicate.RowIndexExcluded} drops exactly the named absolute row positions when read
 * from a multi-row-group file, including positions that straddle a row-group boundary. The predicate alone triggers the
 * row-position synthesis the evaluator needs. Because the synthesized {@code $pos} column never reaches the output,
 * each surviving row's position is recovered from its data (the fixture stores {@code value = position * 1.5}).
 */
class RowIndexExcludedReadIT {

    private static final int ROWS = 10_000;
    private static final ColumnPath VALUE = ColumnPath.of("value");
    private static final ColumnPath V = ColumnPath.of("v");
    // The caller names the synthesized row-position column; the engine mandates no fixed name (Iceberg uses _pos).
    private static final ColumnPath POS = ColumnPath.of("_pos");

    @Test
    void dropsExactlyTheDeletedPositionsAcrossRowGroups(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, ROWS);
        assertThat(TestParquetFiles.rowGroupCount(file))
                .as("fixture must span multiple row groups")
                .isGreaterThan(1);

        long[] deletedPositions = {0L, 5L, 8_191L, 8_192L, 8_193L, 9_999L};
        Predicate predicate = new Predicate.RowIndexExcluded(POS, SortedLongPositionSet.of(deletedPositions));

        List<Long> survivors = collectPositions(file, predicate, RowIndexExcludedReadIT::positionFromValue);

        Set<Long> deleted = new LinkedHashSet<>();
        for (long p : deletedPositions) {
            deleted.add(p);
        }
        List<Long> expected = new ArrayList<>(ROWS - deletedPositions.length);
        for (long i = 0; i < ROWS; i++) {
            if (!deleted.contains(i)) {
                expected.add(i);
            }
        }

        assertThat(survivors).containsExactlyElementsOf(expected);
        assertThat(survivors).doesNotContainAnyElementsOf(deleted);
        assertThat(survivors).hasSize(ROWS - deletedPositions.length);
    }

    @Test
    void aDeletePredicateAloneDoesNotLeakRowPositionIntoOutput(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, ROWS);
        assertThat(TestParquetFiles.rowGroupCount(file))
                .as("fixture must span multiple row groups")
                .isGreaterThan(1);

        long[] deletedPositions = {0L, 5L, 8_191L, 8_192L, 8_193L, 9_999L};
        Predicate predicate = excluding(deletedPositions);

        List<ParquetRecord> records = readRecords(file, predicate, ReadOptions.DEFAULTS);

        assertThat(records).hasSize(ROWS - deletedPositions.length);
        for (ParquetRecord rec : records) {
            assertThat(rec.schema().leafColumns())
                    .as("a delete predicate alone must not add $pos to the output schema")
                    .doesNotContain(POS);
            assertThat(rec.get(POS))
                    .as("the unrequested $pos column must be absent from each materialized record")
                    .isNull();
        }
    }

    @Test
    void countWithADeletePredicateReturnsThePostDeleteCount(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, ROWS);

        long[] deletedPositions = {0L, 5L, 8_191L, 8_192L, 8_193L, 9_999L};
        Predicate predicate = excluding(deletedPositions);

        long count;
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            count = reader.count(predicate, ReadOptions.DEFAULTS);
        }

        assertThat(count).isEqualTo(ROWS - deletedPositions.length);
    }

    @Test
    void appliesDeletesEvenWithRecordLevelFilteringOff(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, ROWS);

        long[] deletedPositions = {0L, 5L, 8_191L, 8_192L, 8_193L, 9_999L};
        Predicate predicate = excluding(deletedPositions);
        ReadOptions pushdownOnly =
                ReadOptions.builder().useRecordLevelFilter(false).build();

        List<Long> survivors =
                collectPositions(file, predicate, pushdownOnly, RowIndexExcludedReadIT::positionFromValue);

        assertThat(survivors)
                .as("positional deletes are exact and apply even when the record-level toggle is off")
                .containsExactlyElementsOf(bruteForceSurvivors(predicate, ROWS));
    }

    @Test
    void countAndReadAgreeWithRecordLevelFilteringOff(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, ROWS);

        long[] deletedPositions = {0L, 5L, 8_191L, 8_192L, 8_193L, 9_999L};
        Predicate predicate = excluding(deletedPositions);
        ReadOptions pushdownOnly =
                ReadOptions.builder().useRecordLevelFilter(false).build();
        long expected = ROWS - deletedPositions.length;

        long count;
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            count = reader.count(predicate, pushdownOnly);
        }
        List<Long> survivors =
                collectPositions(file, predicate, pushdownOnly, RowIndexExcludedReadIT::positionFromValue);

        assertThat(count)
                .as("count applies positional deletes regardless of the record-level toggle")
                .isEqualTo(expected);
        assertThat((long) survivors.size())
                .as("read returns the same surviving count as count")
                .isEqualTo(expected);
    }

    @Test
    void appliesDeletesUnderAnExplicitProjection(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, ROWS);
        long[] deletedPositions = {0L, 5L, 8_191L, 8_192L, 8_193L, 9_999L};
        Predicate predicate = excluding(deletedPositions);
        // An explicit produce set (not ALL) routes through ProjectionPlan.resolveOf, where the synthesized
        // row-position column must be kept out of the physical decode set.
        Projection valueOnly = Projection.ofPhysical(List.of(VALUE));

        List<Long> survivors = new ArrayList<>();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows = reader.read(predicate, valueOnly, ReadOptions.DEFAULTS)) {
                rows.forEach(rec -> {
                    assertThat(rec.schema().leafColumns())
                            .as("the explicit projection is honored and the row-position column never leaks")
                            .containsExactly(VALUE);
                    survivors.add(positionFromValue(rec));
                });
            }
        }

        assertThat(survivors).containsExactlyElementsOf(bruteForceSurvivors(predicate, ROWS));
    }

    @Test
    void deletingTheWholeFirstRowGroupEliminatesIt(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, ROWS);
        List<Long> rowGroupRowCounts = rowGroupRowCounts(file);
        assertThat(rowGroupRowCounts)
                .as("fixture must span multiple row groups")
                .hasSizeGreaterThan(1);
        long firstRowGroupRows = rowGroupRowCounts.get(0);

        Predicate predicate = excluding(absoluteRange(0L, firstRowGroupRows));

        ExplainPlan plan = explain(file, predicate);
        assertThat(plan.rowGroups().get(0).outcome())
                .as("a fully-deleted first row group is eliminated, not decoded")
                .isEqualTo(RowGroupOutcome.ELIMINATED);

        List<Long> survivors = collectPositions(file, predicate, RowIndexExcludedReadIT::positionFromValue);
        List<Long> expected = bruteForceSurvivors(predicate, ROWS);
        assertThat(survivors).containsExactlyElementsOf(expected);
        assertThat(survivors).allMatch(position -> position >= firstRowGroupRows);
    }

    @Test
    void deletingOnlyTheFirstRowGroupLeavesLaterGroupsUntouched(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, ROWS);
        List<Long> rowGroupRowCounts = rowGroupRowCounts(file);
        assertThat(rowGroupRowCounts).hasSizeGreaterThan(1);
        long firstRowGroupRows = rowGroupRowCounts.get(0);

        Predicate predicate = excluding(0L, 5L, firstRowGroupRows - 1);

        ExplainPlan plan = explain(file, predicate);
        assertThat(plan.rowGroups().get(0).outcome())
                .as("the partly-deleted first row group still needs per-row evaluation")
                .isIn(RowGroupOutcome.PARTIAL, RowGroupOutcome.FULL);
        assertThat(everySubsequentRowGroupIsUntouched(plan))
                .as("with no delete touching a later row group, each is matched without per-row work")
                .isTrue();

        List<Long> survivors = collectPositions(file, predicate, RowIndexExcludedReadIT::positionFromValue);
        assertThat(survivors).containsExactlyElementsOf(bruteForceSurvivors(predicate, ROWS));
    }

    @Test
    void deletingAFullPageInsideARowGroupSkipsThatPage(@TempDir Path tmp) throws Exception {
        // A single row group of 12 rows across three 4-row pages. Deleting the middle page [4,8) leaves the row group
        // partial and lets the column-index tier drop that page from the surviving ranges.
        int pageRows = 4;
        Path file = writeSingleRowGroupMultiPageFile(tmp, 3 * pageRows, pageRows);
        long pageStart = pageRows;
        long pageEndExclusive = 2L * pageRows;
        Predicate predicate = excluding(absoluteRange(pageStart, pageEndExclusive));

        ExplainPlan plan = explain(file, predicate);
        RowGroupPlan firstRowGroup = plan.rowGroups().get(0);
        assertThat(firstRowGroup.outcome())
                .as("a fully-deleted page inside a surviving row group narrows it")
                .isEqualTo(RowGroupOutcome.PARTIAL);
        assertThat(pageWasSkipped(firstRowGroup, pageStart, pageEndExclusive))
                .as("the deleted page is dropped from the surviving row ranges")
                .isTrue();

        List<Long> survivors = collectPositions(file, predicate, RowIndexExcludedReadIT::positionFromV);
        assertThat(survivors).containsExactlyElementsOf(bruteForceSurvivors(predicate, 3 * pageRows));
    }

    private static Predicate excluding(long... positions) {
        return new Predicate.RowIndexExcluded(POS, SortedLongPositionSet.of(positions));
    }

    private static long[] absoluteRange(long loInclusive, long hiExclusive) {
        long[] out = new long[(int) (hiExclusive - loInclusive)];
        for (int i = 0; i < out.length; i++) {
            out[i] = loInclusive + i;
        }
        return out;
    }

    private static List<Long> bruteForceSurvivors(Predicate predicate, int rows) {
        Predicate.RowIndexExcluded leaf = (Predicate.RowIndexExcluded) predicate;
        List<Long> expected = new ArrayList<>(rows);
        for (long position = 0; position < rows; position++) {
            if (!leaf.deleted().contains(position)) {
                expected.add(position);
            }
        }
        return expected;
    }

    private static ExplainPlan explain(Path file, Predicate predicate) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            return reader.explain(predicate, Projection.ALL, ReadOptions.DEFAULTS);
        }
    }

    private static boolean everySubsequentRowGroupIsUntouched(ExplainPlan plan) {
        List<RowGroupPlan> rowGroups = plan.rowGroups();
        for (int index = 1; index < rowGroups.size(); index++) {
            if (rowGroups.get(index).outcome() != RowGroupOutcome.MATCHED) {
                return false;
            }
        }
        return true;
    }

    private static boolean pageWasSkipped(RowGroupPlan rowGroup, long pageStart, long pageEndExclusive) {
        boolean narrowedAtColumnIndex = rowGroup.tiers().stream()
                .anyMatch(tier ->
                        tier instanceof PruningDecision.NarrowedTo narrowed && narrowed.tier() == Tier.COLUMN_INDEX);
        RowRanges surviving = rowGroup.survivingRows().orElseThrow();
        boolean noSurvivingRowInDeletedPage = surviving.ranges().stream()
                .noneMatch(range -> range.first() < pageEndExclusive && range.last() >= pageStart);
        return narrowedAtColumnIndex && noSurvivingRowInDeletedPage;
    }

    private static List<Long> rowGroupRowCounts(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            return footer.rowGroups().stream().map(RowGroup::numRows).toList();
        }
    }

    /**
     * Writes a single-row-group file of {@code rows} INT32 values, capping each data page at {@code pageRows} values so
     * the row group holds several pages with an offset index the page tier can reason about.
     */
    private static Path writeSingleRowGroupMultiPageFile(Path dir, int rows, int pageRows) throws Exception {
        Path file = dir.resolve("single-rg-multi-page.parquet");
        ColumnPath valueColumn = ColumnPath.of("v");
        SchemaNode.Primitive valueLeaf = new SchemaNode.Primitive(
                "v", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(valueLeaf), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(root);
        WriteOptions options =
                WriteOptions.builder().tempDir(dir).pageValueLimit(pageRows).build();
        List<Map<ColumnPath, Object>> values = new ArrayList<>(rows);
        for (int row = 0; row < rows; row++) {
            values.add(Map.of(valueColumn, row));
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.writeBatch(WriteFixtures.batch(schema, values));
        }
        return file;
    }

    /**
     * The absolute file positions of the rows that survive {@code predicate}, recovered from each surviving row's data
     * via {@code positionOf}. The synthesized {@code $pos} column is filter-only and never reaches the output. The
     * survivors are read through real columns instead.
     */
    private static List<Long> collectPositions(
            Path file, Predicate predicate, ToLongFunction<ParquetRecord> positionOf) {
        return collectPositions(file, predicate, ReadOptions.DEFAULTS, positionOf);
    }

    private static List<Long> collectPositions(
            Path file, Predicate predicate, ReadOptions options, ToLongFunction<ParquetRecord> positionOf) {
        List<Long> positions = new ArrayList<>();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, options)) {
                rows.forEach(rec -> positions.add(positionOf.applyAsLong(rec)));
            }
        }
        return positions;
    }

    private static List<ParquetRecord> readRecords(Path file, Predicate predicate, ReadOptions options) {
        List<ParquetRecord> records = new ArrayList<>();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, options)) {
                rows.forEach(rec -> records.add(rec.detach()));
            }
        }
        return records;
    }

    /** The three-column fixture stores {@code value = position * 1.5}; recover the row's absolute file position. */
    private static long positionFromValue(ParquetRecord rec) {
        return Math.round(rec.getDouble(VALUE) / 1.5);
    }

    /** The single-row-group multi-page fixture stores {@code v = position} directly. */
    private static long positionFromV(ParquetRecord rec) {
        return rec.getInt(V);
    }
}
