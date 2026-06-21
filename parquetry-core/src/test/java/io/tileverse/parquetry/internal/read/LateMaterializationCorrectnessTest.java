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
package io.tileverse.parquetry.internal.read;

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetReader;
import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Parameterized correctness sweep for the late-materializing read path. Every case reads rows through the full
 * late-materialization path and compares against a brute-force baseline that decodes all rows then filters them in Java
 * using the same predicate logic.
 *
 * <p>The fixture file contains:
 *
 * <ul>
 *   <li>{@code id} INT64 REQUIRED - sorted predicate key
 *   <li>{@code i32} INT32 OPTIONAL - null for every third row
 *   <li>{@code i64} INT64 REQUIRED
 *   <li>{@code d} DOUBLE REQUIRED
 *   <li>{@code name} BYTE_ARRAY OPTIONAL - null for every fifth row
 * </ul>
 *
 * <p>Small page value limit and multiple row groups ensure both page-pruning and full row-group skipping are exercised.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LateMaterializationCorrectnessTest {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath I32 = ColumnPath.of("i32");
    private static final ColumnPath I64 = ColumnPath.of("i64");
    private static final ColumnPath D = ColumnPath.of("d");
    private static final ColumnPath NAME = ColumnPath.of("name");

    /** Total rows in the fixture. Multiple of the row-group size so that full groups exist on both ends. */
    private static final int ROW_COUNT = 120;

    /** Every Nth row has a null {@code i32}. Chosen to straddle page and row-group boundaries. */
    private static final int I32_NULL_STRIDE = 3;

    /** Every Nth row has a null {@code name}. Different stride ensures null patterns don't coincide everywhere. */
    private static final int NAME_NULL_STRIDE = 5;

    /** Populated by {@code @BeforeAll} via parameter injection; shared across all parameterized cases. */
    private Path fixtureDir;

    /** Shared fixture file, written once per test class. */
    private Path fixtureFile;

    // ---------------------------------------------------------------------------
    // Parameter record
    // ---------------------------------------------------------------------------

    record Case(String label, Predicate predicate, Projection projection, int expectedCount) {
        @Override
        public String toString() {
            return label;
        }
    }

    // ---------------------------------------------------------------------------
    // Fixture setup
    // ---------------------------------------------------------------------------

    @BeforeAll
    void writeFixture(@TempDir Path tempDir) throws Exception {
        fixtureDir = tempDir;
        fixtureFile = tempDir.resolve("lm-correctness.parquet");
        ParquetSchema schema = buildSchema();
        // Small page limit and 30-row groups produce four row groups and many pages,
        // making both partial-group and full-group pruning happen across predicates.
        WriteOptions options = WriteOptions.builder()
                .tempDir(fixtureDir)
                .rowGroupSize(WriteOptions.RowGroupSize.rows(30))
                .pageValueLimit(8)
                .build();
        // One batch per row keeps the writer's per-row 30-row-group boundaries, which the pruning cases rely on.
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(fixtureFile), schema, options)) {
            for (long id = 0; id < ROW_COUNT; id++) {
                writer.writeBatch(WriteFixtures.batch(schema, List.of(buildRow(id))));
            }
        }
    }

    // ---------------------------------------------------------------------------
    // Test cases
    // ---------------------------------------------------------------------------

    static Stream<Case> predicateCases() {
        return Stream.of(
                // point lookup - exactly one row
                new Case("point-lookup-id=50", col("id").eq(50L), Projection.of(Set.of(I32, I64, D, NAME)), 1),
                // point lookup including the predicate column in the projection
                new Case(
                        "point-lookup-id=50-with-id-projected",
                        col("id").eq(50L),
                        Projection.of(Set.of(ID, I32, D)),
                        1),
                // mid-file contiguous range spanning multiple pages
                new Case(
                        "range-id-30-to-59",
                        col("id").gtEq(30L).and(col("id").lt(60L)),
                        Projection.of(Set.of(I32, I64, D, NAME)),
                        30),
                // range that includes rows where i32 is null (straddles null / non-null output)
                new Case(
                        "range-straddling-nulls-id-10-to-25",
                        col("id").gtEq(10L).and(col("id").lt(25L)),
                        Projection.of(Set.of(ID, I32, NAME)),
                        15),
                // sparse range hitting page boundaries (first page of each row group)
                new Case("sparse-range-id-lt-10", col("id").lt(10L), Projection.of(Set.of(I32, I64, D, NAME)), 10),
                // range covering exactly the last row group
                new Case(
                        "last-row-group-id-90-to-119",
                        col("id").gtEq(90L).and(col("id").lt(120L)),
                        Projection.of(Set.of(I32, I64, D)),
                        30),
                // all rows - late materialization should not activate for ALWAYS_TRUE
                new Case(
                        "all-rows-always-true",
                        Predicate.ALWAYS_TRUE,
                        Projection.of(Set.of(I32, I64, D, NAME)),
                        ROW_COUNT),
                // no rows - predicate matches nothing
                new Case("no-match-id-gt-999", col("id").gt(999L), Projection.of(Set.of(I32, I64, D, NAME)), 0),
                // output-only projection (predicate column absent from output)
                new Case(
                        "output-only-no-id-in-projection",
                        col("id").gtEq(100L),
                        Projection.of(Set.of(I32, I64, D, NAME)),
                        20));
    }

    // ---------------------------------------------------------------------------
    // Parameterized test: late-mat read equals brute-force baseline
    // ---------------------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("predicateCases")
    void lateMaterializationMatchesBaseline(Case tc) {
        List<Row> actual = readRows(tc.predicate(), tc.projection(), ReadOptions.DEFAULTS);
        List<Row> expected = buildBaseline(tc.predicate(), tc.projection());

        assertThat(actual)
                .as("%s: row count", tc.label())
                .hasSize(tc.expectedCount())
                .as("%s: late-mat rows equal brute-force baseline (same count, values, order)", tc.label())
                .containsExactlyElementsOf(expected);
    }

    // ---------------------------------------------------------------------------
    // Parameterized test: nullable output columns carry nulls in the right positions
    // ---------------------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("predicateCases")
    void nullableOutputColumnsAreCorrect(Case tc) {
        List<Row> actual = readRows(tc.predicate(), tc.projection(), ReadOptions.DEFAULTS);

        Set<ColumnPath> kept = resolveKept(tc.projection());
        if (!kept.contains(I32) && !kept.contains(NAME)) {
            // projection excludes both nullable columns; nothing to verify here
            return;
        }

        for (Row row : actual) {
            if (row.id() != null) {
                long id = row.id();
                if (kept.contains(I32)) {
                    boolean expectNull = (id % I32_NULL_STRIDE == 0);
                    if (expectNull) {
                        assertThat(row.i32())
                                .as("i32 must be null for id=%d", id)
                                .isNull();
                    } else {
                        assertThat(row.i32())
                                .as("i32 must be non-null for id=%d", id)
                                .isNotNull()
                                .isEqualTo(i32ValueFor(id));
                    }
                }
                if (kept.contains(NAME)) {
                    boolean expectNull = (id % NAME_NULL_STRIDE == 0);
                    if (expectNull) {
                        assertThat(row.name())
                                .as("name must be null for id=%d", id)
                                .isNull();
                    } else {
                        assertThat(row.name())
                                .as("name must be non-null for id=%d", id)
                                .isNotNull()
                                .isEqualTo(nameValueFor(id));
                    }
                }
            }
        }
    }

    // ---------------------------------------------------------------------------
    // readBatches unaffected: flattened batches equal read-with-record-filter-off
    // ---------------------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("predicateCases")
    void readBatchesIsUnaffectedByRecordFilter(Case tc) {
        // read() with record filter disabled - page-pruned superset only
        ReadOptions noRecordFilter =
                ReadOptions.builder().useRecordLevelFilter(false).build();
        List<Row> readNoFilter = readRows(tc.predicate(), tc.projection(), noRecordFilter);

        // readBatches() with the same predicate and projection - should equal the no-record-filter read
        List<Row> batchRows = flattenBatches(tc.predicate(), tc.projection(), noRecordFilter);

        assertThat(batchRows)
                .as(
                        "%s: readBatches flattened equals read-with-record-filter-off (batch path applies no per-row"
                                + " filter)",
                        tc.label())
                .containsExactlyElementsOf(readNoFilter);

        // For non-trivially-true predicates also verify that readBatches is a superset of the matching rows
        if (tc.predicate() != Predicate.ALWAYS_TRUE && tc.expectedCount() > 0 && tc.expectedCount() < ROW_COUNT) {
            List<Row> matchingRows = readRows(tc.predicate(), tc.projection(), ReadOptions.DEFAULTS);
            assertThat(batchRows)
                    .as("%s: readBatches result is a superset of the late-mat-filtered rows", tc.label())
                    .containsAll(matchingRows);
        }
    }

    // ---------------------------------------------------------------------------
    // Read helpers
    // ---------------------------------------------------------------------------

    private List<Row> readRows(Predicate predicate, Projection projection, ReadOptions options) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(fixtureFile)) {
            ParquetReader dataset = ParquetReader.open(source);
            try (Stream<ParquetRecord> rows = dataset.read(predicate, projection, options)) {
                return rows.map(rec -> toRow(rec, projection)).toList();
            }
        }
    }

    private List<Row> flattenBatches(Predicate predicate, Projection projection, ReadOptions options) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(fixtureFile)) {
            ParquetReader dataset = ParquetReader.open(source);
            List<Row> result = new ArrayList<>();
            try (Stream<ParquetRecordBatch> batches = dataset.readBatches(predicate, projection, options)) {
                List<ParquetRecordBatch> collected = batches.toList();
                for (ParquetRecordBatch batch : collected) {
                    for (int i = 0; i < batch.rowCount(); i++) {
                        ParquetRecord rec = batch.materialize(i);
                        result.add(toRow(rec, projection));
                    }
                    batch.close();
                }
            }
            return result;
        }
    }

    // ---------------------------------------------------------------------------
    // Baseline builder (pure-Java filter over the known data, independent of the reader)
    // ---------------------------------------------------------------------------

    private List<Row> buildBaseline(Predicate predicate, Projection projection) {
        List<Row> matching = new ArrayList<>();
        for (long id = 0; id < ROW_COUNT; id++) {
            if (evaluatePredicate(predicate, id)) {
                matching.add(expectedRow(id, projection));
            }
        }
        return matching;
    }

    /**
     * Evaluates the predicates used in this test against the known row contents. Keeps the baseline independent of the
     * reader under test.
     */
    @SuppressWarnings("java:S7475") // palantirJavaFormat:2.90 doesn't support removing unused type from unnamed pattern
    private static boolean evaluatePredicate(Predicate predicate, long id) {
        return switch (predicate) {
            case Predicate.Always(boolean value) -> value;
            case Predicate.Eq(ColumnPath _, var v) -> id == longOf(v);
            case Predicate.Gt(ColumnPath _, var v) -> id > longOf(v);
            case Predicate.GtEq(ColumnPath _, var v) -> id >= longOf(v);
            case Predicate.Lt(ColumnPath _, var v) -> id < longOf(v);
            case Predicate.LtEq(ColumnPath _, var v) -> id <= longOf(v);
            case Predicate.And(List<Predicate> children) ->
                children.stream().allMatch(child -> evaluatePredicate(child, id));
            case Predicate.Or(List<Predicate> children) ->
                children.stream().anyMatch(child -> evaluatePredicate(child, id));
            default -> throw new IllegalArgumentException("Unsupported predicate in baseline: " + predicate);
        };
    }

    private static long longOf(io.tileverse.parquetry.filter.Value v) {
        return switch (v) {
            case io.tileverse.parquetry.filter.Value.LongVal(long n) -> n;
            default -> throw new IllegalArgumentException("Expected a long value, got " + v);
        };
    }

    // ---------------------------------------------------------------------------
    // Row model and conversion helpers
    // ---------------------------------------------------------------------------

    private record Row(Long id, Integer i32, Long i64, Double d, String name) {}

    private Row expectedRow(long id, Projection projection) {
        Set<ColumnPath> kept = resolveKept(projection);
        Long projId = kept.contains(ID) ? id : null;
        Integer projI32 = kept.contains(I32) ? i32ValueForNullable(id) : null;
        Long projI64 = kept.contains(I64) ? i64ValueFor(id) : null;
        Double projD = kept.contains(D) ? dValueFor(id) : null;
        String projName = kept.contains(NAME) ? nameValueForNullable(id) : null;
        return new Row(projId, projI32, projI64, projD, projName);
    }

    private Row toRow(ParquetRecord rec, Projection projection) {
        Set<ColumnPath> kept = resolveKept(projection);
        Long id = kept.contains(ID) ? rec.getLong(ID) : null;
        Integer i32 = kept.contains(I32) ? readNullableInt(rec, I32) : null;
        Long i64 = kept.contains(I64) ? rec.getLong(I64) : null;
        Double d = kept.contains(D) ? rec.getDouble(D) : null;
        String name = kept.contains(NAME) ? readNullableString(rec, NAME) : null;
        return new Row(id, i32, i64, d, name);
    }

    private static Integer readNullableInt(ParquetRecord rec, ColumnPath col) {
        if (rec.isNull(col)) {
            return null;
        }
        return rec.getInt(col);
    }

    private static String readNullableString(ParquetRecord rec, ColumnPath col) {
        if (rec.isNull(col)) {
            return null;
        }
        return rec.getString(col);
    }

    private static Set<ColumnPath> resolveKept(Projection projection) {
        return switch (projection) {
            case Projection.All _ -> Set.of(ID, I32, I64, D, NAME);
            case Projection.Columns(Set<ColumnPath> kept) -> kept;
        };
    }

    // ---------------------------------------------------------------------------
    // Value functions (deterministic from id)
    // ---------------------------------------------------------------------------

    private static int i32ValueFor(long id) {
        return (int) (id * 7);
    }

    private static Integer i32ValueForNullable(long id) {
        if (id % I32_NULL_STRIDE == 0) {
            return null;
        }
        return i32ValueFor(id);
    }

    private static long i64ValueFor(long id) {
        return id * 100_000L;
    }

    private static double dValueFor(long id) {
        return id * 3.14;
    }

    private static String nameValueFor(long id) {
        return "entry-" + id;
    }

    private static String nameValueForNullable(long id) {
        if (id % NAME_NULL_STRIDE == 0) {
            return null;
        }
        return nameValueFor(id);
    }

    // ---------------------------------------------------------------------------
    // Schema and write-row helpers
    // ---------------------------------------------------------------------------

    private static ParquetSchema buildSchema() {
        List<SchemaNode> children = List.of(
                requiredLeaf("id", PrimitiveKind.INT64),
                optionalLeaf("i32", PrimitiveKind.INT32),
                requiredLeaf("i64", PrimitiveKind.INT64),
                requiredLeaf("d", PrimitiveKind.DOUBLE),
                optionalLeaf("name", PrimitiveKind.BYTE_ARRAY));
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredLeaf(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.REQUIRED, kind, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive optionalLeaf(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static Map<ColumnPath, Object> buildRow(long id) {
        Map<ColumnPath, Object> cells = new HashMap<>();
        cells.put(ID, id);
        cells.put(I32, id % I32_NULL_STRIDE == 0 ? null : i32ValueFor(id));
        cells.put(I64, i64ValueFor(id));
        cells.put(D, dValueFor(id));
        cells.put(NAME, id % NAME_NULL_STRIDE == 0 ? null : nameValueFor(id).getBytes(StandardCharsets.UTF_8));
        return cells;
    }
}
