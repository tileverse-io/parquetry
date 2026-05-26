/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.data.read;

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
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

import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteRow;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * End-to-end coverage of the late-materializing read path through the public {@link ParquetDataset} API. Every case
 * compares the read against a brute-force baseline that decodes all rows and filters them in Java with the same
 * predicate, asserting identical rows in identical order.
 */
class LateMaterializationReadTest {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath V = ColumnPath.of("v");
    private static final ColumnPath NAME = ColumnPath.of("name");
    private static final ColumnPath TAG = ColumnPath.of("tag");

    private static final int ROW_COUNT = 100;

    @TempDir
    Path tempDir;

    @Test
    void selectivePredicateWithOutputOnlyColumnsMatchesBaseline() throws Exception {
        Path file = writeRows(WriteOptions.RowGroupSize.adaptive());
        Predicate predicate = col("id").gtEq(40L).and(col("id").lt(50L));
        Projection projection = Projection.of(Set.of(V, NAME));

        List<Row> actual = readRows(file, predicate, projection, ReadOptions.DEFAULTS);
        List<Row> expected = baseline(predicate, projection);

        assertThat(actual)
                .as("output-only projection yields exactly the predicate-matching rows, in order")
                .containsExactlyElementsOf(expected);
        assertThat(actual).as("ten rows match id in [40, 50)").hasSize(10);
    }

    @Test
    void predicateColumnAlsoProjectedMatchesBaseline() throws Exception {
        Path file = writeRows(WriteOptions.RowGroupSize.adaptive());
        Predicate predicate = col("id").eq(73L);
        Projection projection = Projection.of(Set.of(ID, V));

        List<Row> actual = readRows(file, predicate, projection, ReadOptions.DEFAULTS);
        List<Row> expected = baseline(predicate, projection);

        assertThat(actual)
                .as("the predicate column appears in the output with its matching value")
                .containsExactlyElementsOf(expected);
        assertThat(actual).hasSize(1);
        assertThat(actual.get(0).id())
                .as("the projected id carries the matched value")
                .isEqualTo(73L);
    }

    @Test
    void predicateColumnNotProjectedIsAbsentFromOutput() throws Exception {
        Path file = writeRows(WriteOptions.RowGroupSize.adaptive());
        Predicate predicate = col("id").gtEq(95L);
        Projection projection = Projection.of(Set.of(TAG));

        List<Row> actual = readRows(file, predicate, projection, ReadOptions.DEFAULTS);
        List<Row> expected = baseline(predicate, projection);

        assertThat(actual).containsExactlyElementsOf(expected);
        assertThat(actual).as("five rows match id >= 95").hasSize(5);
        assertThat(actual)
                .as("the predicate column is not in the projection, hence absent from output")
                .allSatisfy(row -> assertThat(row.id()).isNull());
    }

    @Test
    void recordFilterOffReturnsNoFilteredSuperset() throws Exception {
        Path file = writeRows(WriteOptions.RowGroupSize.adaptive());
        Predicate predicate = col("id").gtEq(40L).and(col("id").lt(50L));
        Projection projection = Projection.of(Set.of(V, NAME));
        ReadOptions noRecordFilter =
                ReadOptions.builder().useRecordLevelFilter(false).build();
        ReadOptions noFiltering = ReadOptions.builder()
                .useRecordLevelFilter(false)
                .useColumnIndexFilter(false)
                .build();

        List<Row> matching = baseline(predicate, projection);
        List<Row> pagePruned = readRows(file, predicate, projection, noRecordFilter);
        List<Row> unfiltered = readRows(file, predicate, projection, noFiltering);

        assertThat(unfiltered)
                .as("with both the record-level and page tiers off every row is returned, none dropped")
                .hasSize(ROW_COUNT);
        assertThat(pagePruned)
                .as("the record filter off path keeps every matching row, only pages were pruned")
                .containsAll(matching);
        assertThat(pagePruned)
                .as("no per-row filtering happened, hence the result is a superset of the ten matching rows")
                .hasSizeGreaterThan(matching.size());
    }

    @Test
    void triviallyTruePredicateReturnsEveryRow() throws Exception {
        Path file = writeRows(WriteOptions.RowGroupSize.adaptive());
        Projection projection = Projection.of(Set.of(V, NAME));

        List<Row> actual = readRows(file, Predicate.ALWAYS_TRUE, projection, ReadOptions.DEFAULTS);

        assertThat(actual)
                .as("a trivially-true predicate is not eligible for late materialization and returns every row")
                .hasSize(ROW_COUNT);
    }

    @Test
    void multiRowGroupSelectivePredicateMatchesBaseline() throws Exception {
        Path file = writeRows(WriteOptions.RowGroupSize.rows(25));
        Predicate predicate = col("id").gtEq(10L).and(col("id").lt(80L));
        Projection projection = Projection.of(Set.of(V, NAME));

        List<Row> actual = readRows(file, predicate, projection, ReadOptions.DEFAULTS);
        List<Row> expected = baseline(predicate, projection);

        assertThat(actual)
                .as("matching rows are collected in order across multiple row groups")
                .containsExactlyElementsOf(expected);
        assertThat(actual).as("seventy rows match id in [10, 80)").hasSize(70);
    }

    @Test
    void disablingLateMaterializationKeepsResultsIdentical() throws Exception {
        Path file = writeRows(WriteOptions.RowGroupSize.rows(25));
        Predicate predicate = col("id").gtEq(10L).and(col("id").lt(80L));
        Projection projection = Projection.of(Set.of(V, NAME));
        ReadOptions lateMatOff =
                ReadOptions.builder().useLateMaterialization(false).build();

        List<Row> withLateMat = readRows(file, predicate, projection, ReadOptions.DEFAULTS);
        List<Row> withoutLateMat = readRows(file, predicate, projection, lateMatOff);
        List<Row> expected = baseline(predicate, projection);

        assertThat(withLateMat)
                .as("the two-phase path and the full-decode fallback return the same matching rows")
                .containsExactlyElementsOf(withoutLateMat);
        assertThat(withoutLateMat)
                .as("the fallback still applies the record filter and matches the baseline")
                .containsExactlyElementsOf(expected);
    }

    // --- read + baseline helpers ---

    private List<Row> readRows(Path file, Predicate predicate, Projection projection, ReadOptions options)
            throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetDataset dataset = ParquetDataset.open(source);
            try (Stream<ParquetRecord> rows = dataset.read(predicate, projection, options)) {
                return rows.map(record -> toRow(record, projection)).toList();
            }
        }
    }

    /** Decodes the file's known contents in Java and keeps the rows the predicate accepts, in row order. */
    private List<Row> baseline(Predicate predicate, Projection projection) {
        List<Row> matching = new ArrayList<>();
        for (long id = 0; id < ROW_COUNT; id++) {
            if (matchesSelectiveRange(predicate, id)) {
                matching.add(projectedRow(id, projection));
            }
        }
        return matching;
    }

    /**
     * Evaluates the small family of {@code id}-range predicates this test uses against the known row contents, which
     * keeps the baseline independent of the reader under test.
     */
    private static boolean matchesSelectiveRange(Predicate predicate, long id) {
        return switch (predicate) {
            case Predicate.Eq(ColumnPath _, var value) -> id == ((Number) longOf(value)).longValue();
            case Predicate.GtEq(ColumnPath _, var value) -> id >= longOf(value);
            case Predicate.Lt(ColumnPath _, var value) -> id < longOf(value);
            case Predicate.And(List<Predicate> children) ->
                children.stream().allMatch(child -> matchesSelectiveRange(child, id));
            default -> throw new IllegalArgumentException("Unsupported predicate in baseline: " + predicate);
        };
    }

    private static long longOf(Object value) {
        return switch (value) {
            case io.tileverse.parquetry.filter.Value.LongVal(long v) -> v;
            default -> throw new IllegalArgumentException("Expected a long value, got " + value);
        };
    }

    private Row projectedRow(long id, Projection projection) {
        Set<ColumnPath> kept = keptColumns(projection);
        Long projectedId = kept.contains(ID) ? id : null;
        Double projectedV = kept.contains(V) ? id * 1.5 : null;
        String projectedName = kept.contains(NAME) ? "row-" + id : null;
        Integer projectedTag = kept.contains(TAG) ? (int) (id * 10) : null;
        return new Row(projectedId, projectedV, projectedName, projectedTag);
    }

    private Row toRow(ParquetRecord record, Projection projection) {
        Set<ColumnPath> kept = keptColumns(projection);
        Long id = kept.contains(ID) ? record.getLong(ID) : null;
        Double v = kept.contains(V) ? record.getDouble(V) : null;
        String name = kept.contains(NAME) ? record.getString(NAME) : null;
        Integer tag = kept.contains(TAG) ? record.getInt(TAG) : null;
        return new Row(id, v, name, tag);
    }

    private static Set<ColumnPath> keptColumns(Projection projection) {
        return switch (projection) {
            case Projection.All _ -> Set.of(ID, V, NAME, TAG);
            case Projection.Columns(Set<ColumnPath> kept) -> kept;
        };
    }

    private record Row(Long id, Double v, String name, Integer tag) {}

    // --- file writing ---

    private Path writeRows(WriteOptions.RowGroupSize rowGroupSize) throws Exception {
        Path file = tempDir.resolve("late-materialization-read.parquet");
        ParquetSchema schema = fileSchema();
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(rowGroupSize)
                .pageValueLimit(8)
                .build();
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            for (long id = 0; id < ROW_COUNT; id++) {
                writer.write(row(id));
            }
        }
        return file;
    }

    private static WriteRow row(long id) {
        Map<ColumnPath, Object> cells =
                Map.of(ID, id, V, id * 1.5, NAME, ("row-" + id).getBytes(StandardCharsets.UTF_8), TAG, (int) (id * 10));
        return cells::get;
    }

    private static ParquetSchema fileSchema() {
        return flatSchema(
                requiredLeaf("id", PrimitiveKind.INT64),
                requiredLeaf("v", PrimitiveKind.DOUBLE),
                requiredLeaf("name", PrimitiveKind.BYTE_ARRAY),
                requiredLeaf("tag", PrimitiveKind.INT32));
    }

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children =
                Stream.of(leaves).map(leaf -> (SchemaNode) leaf).toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredLeaf(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.REQUIRED, kind, OptionalInt.empty(), Optional.empty(), -1);
    }
}
