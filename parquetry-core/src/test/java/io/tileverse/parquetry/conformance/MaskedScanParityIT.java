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
package io.tileverse.parquetry.conformance;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.internal.filter.RecordAccessors;
import io.tileverse.parquetry.internal.filter.RecordLevelEvaluator;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.CorpusFixtures;
import io.tileverse.parquetry.testsupport.Wkb;

/**
 * The acceptance oracle for a filtered read: over a matrix of corpus fixtures, batch sizes, page-index settings,
 * projections and predicates, the rows a filtered read returns must equal, one for one and in file order, the rows a
 * reference produces by decoding every column unfiltered and testing each materialized row in Java. The same matrix
 * pins {@code count(predicate)} to the surviving row count and, over the written GeoParquet fixture, the folded bounds
 * to the box of the reference-surviving points.
 *
 * <p>Each case names its own arms. The batch-size arm is either one row per batch or the unbounded default; the
 * page-index arm is either the default (the COLUMN_INDEX tier narrows the read to the surviving pages, which puts the
 * scan under a row mask) or {@code useColumnIndexFilter=false}, which leaves the scan to walk every page of the row
 * group and hence to follow rows that outrun a page boundary.
 */
class MaskedScanParityIT {

    private static final String GEO_FIXTURE = "written:geo-points";
    private static final int GEO_POINTS = 24;

    @TempDir
    static Path sharedTempDir;

    private static Path geoFile;

    @BeforeAll
    static void writeGeoFixture() throws Exception {
        geoFile = writeGeoPoints(sharedTempDir);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void aFilteredReadReturnsTheReferenceRows(Case testCase) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(fileOf(testCase))) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            Projection projection = projectionOf(reader.schema(), testCase.projectedColumns());
            List<ColumnPath> compared = comparedColumns(testCase.projectedColumns());
            ReadOptions options = optionsOf(testCase);

            List<List<Object>> reference = referenceRows(reader, testCase.predicate(), compared);

            assertThat(readRows(reader, testCase.predicate(), projection, options, compared))
                    .as("read")
                    .isEqualTo(reference);
            assertThat(batchRows(reader, testCase.predicate(), projection, options, compared))
                    .as("readBatches")
                    .isEqualTo(reference);
            assertThat(reader.count(testCase.predicate(), options)).as("count").isEqualTo(reference.size());
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("geoCases")
    void foldedBoundsCoverExactlyTheReferenceRows(Case testCase) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(fileOf(testCase))) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            ReadOptions options = optionsOf(testCase);
            List<Long> surviving = referenceGeoIds(reader, testCase.predicate());

            Optional<BoundingBox> bounds = reader.bounds(testCase.predicate(), options);

            assertThat(boxOf(bounds)).as("folded bounds").isEqualTo(pointsBoxOf(surviving));
        }
    }

    // --- the case matrix ---

    /**
     * One read to check: a fixture, what to ask of it, and the arms it runs under. {@code projectedColumns} names
     * top-level columns, which the test resolves to the file's leaves under them; a whole top-level column keeps the
     * reference comparable value for value against the read's narrower output.
     */
    record Case(
            String label,
            String fixture,
            Predicate predicate,
            List<String> projectedColumns,
            OptionalInt batchSize,
            boolean withIndex) {

        @Override
        public String toString() {
            return label;
        }
    }

    /** Every scenario under both batch-size arms and both page-index arms. */
    static List<Case> cases() {
        return expand(scenarios());
    }

    static List<Case> geoCases() {
        return expand(geoScenarios());
    }

    private static List<Case> expand(List<Case> scenarios) {
        List<Case> cases = new ArrayList<>();
        for (Case scenario : scenarios) {
            cases.add(armOf(scenario, OptionalInt.of(1), true));
            cases.add(armOf(scenario, OptionalInt.of(1), false));
            cases.add(armOf(scenario, OptionalInt.empty(), true));
            cases.add(armOf(scenario, OptionalInt.empty(), false));
        }
        return cases;
    }

    private static Case armOf(Case scenario, OptionalInt batchSize, boolean withIndex) {
        String label = scenario.label() + " | " + describeBatchSize(batchSize) + " | " + describeIndex(withIndex);
        return new Case(
                label, scenario.fixture(), scenario.predicate(), scenario.projectedColumns(), batchSize, withIndex);
    }

    private static String describeBatchSize(OptionalInt batchSize) {
        if (batchSize.isPresent()) {
            return "batchSize=" + batchSize.getAsInt();
        }
        return "batchSize=unbounded";
    }

    private static String describeIndex(boolean withIndex) {
        if (withIndex) {
            return "page index on";
        }
        return "page index off (useColumnIndexFilter=false)";
    }

    /**
     * The reads worth checking, each labelled by fixture, predicate selectivity and projection shape. The corpus
     * fixtures cover nullable flat columns, three-level lists, nested lists of lists, maps of maps, a legacy repeated
     * group with no LIST annotation, and v2 data pages.
     */
    private static List<Case> scenarios() {
        List<Case> scenarios = new ArrayList<>();
        scenarios.addAll(impalaScenarios());
        scenarios.addAll(nestedListAndMapScenarios());
        scenarios.addAll(repeatedAndDataPageV2Scenarios());
        scenarios.addAll(geoScenarios());
        return scenarios;
    }

    /** {@code nullable.impala.parquet}: ids 1..7 beside lists, maps and a deeply nested struct. */
    private static List<Case> impalaScenarios() {
        String fixture = "nullable.impala.parquet";
        return List.of(
                scenario(
                        "impala selective, flat projection",
                        fixture,
                        Pred.col("id").gtEq(5L),
                        List.of("id")),
                scenario(
                        "impala selective, nested projection",
                        fixture,
                        Pred.col("id").gtEq(5L),
                        List.of("int_array", "nested_struct")),
                scenario(
                        "impala selective, mixed projection",
                        fixture,
                        Pred.col("id").gtEq(5L),
                        List.of("id", "int_map", "nested_struct")),
                scenario(
                        "impala all-match, mixed projection",
                        fixture,
                        Pred.col("id").gtEq(1L),
                        List.of("id", "int_array_Array")),
                scenario(
                        "impala no-match, mixed projection",
                        fixture,
                        Pred.col("id").gt(100L),
                        List.of("id", "int_array")));
    }

    /** Nested lists of lists, maps of maps, and a list-only file with no flat column to filter on. */
    private static List<Case> nestedListAndMapScenarios() {
        return List.of(
                scenario(
                        "nested_lists all-match, nested projection",
                        "nested_lists.snappy.parquet",
                        Pred.col("b").eq(1),
                        List.of("a")),
                scenario(
                        "nested_lists no-match, mixed projection",
                        "nested_lists.snappy.parquet",
                        Pred.col("b").eq(2),
                        List.of("a", "b")),
                scenario(
                        "nested_maps all-match, mixed projection",
                        "nested_maps.snappy.parquet",
                        Pred.col("b").eq(1),
                        List.of("a", "b", "c")),
                scenario(
                        "nested_maps no-match, nested projection",
                        "nested_maps.snappy.parquet",
                        Pred.col("c").eq(2.0),
                        List.of("a")),
                scenario(
                        "list_columns unfiltered, nested projection",
                        "list_columns.parquet",
                        Predicate.ALWAYS_TRUE,
                        List.of("int64_list", "utf8_list")));
    }

    /** A legacy repeated group with no LIST annotation, and a file written with v2 data pages. */
    private static List<Case> repeatedAndDataPageV2Scenarios() {
        String repeated = "repeated_no_annotation.parquet";
        String dataPageV2 = "datapage_v2.snappy.parquet";
        return List.of(
                scenario(
                        "repeated_no_annotation selective, mixed projection",
                        repeated,
                        Pred.col("id").gtEq(4),
                        List.of("id", "phoneNumbers")),
                scenario(
                        "repeated_no_annotation selective, nested projection",
                        repeated,
                        Pred.col("id").gtEq(4),
                        List.of("phoneNumbers")),
                scenario(
                        "datapage_v2 selective, mixed projection",
                        dataPageV2,
                        Pred.col("b").gtEq(4),
                        List.of("a", "b", "e")),
                scenario(
                        "datapage_v2 selective on a nullable column, flat projection",
                        dataPageV2,
                        Pred.col("a").isNotNull(),
                        List.of("a", "b")),
                scenario(
                        "datapage_v2 no-match, mixed projection",
                        dataPageV2,
                        Pred.col("b").gt(100),
                        List.of("b", "e")));
    }

    /** The written GeoParquet fixture, whose bbox covering columns lower a spatial leaf to numeric comparisons. */
    private static List<Case> geoScenarios() {
        Bbox lowerHalf = Bbox.of2d(0.0, 0.0, 10.0, 20.0);
        Bbox offMap = Bbox.of2d(1000.0, 1000.0, 2000.0, 2000.0);
        return List.of(
                scenario(
                        "geo selective on an attribute, mixed projection",
                        GEO_FIXTURE,
                        Pred.col("id").gtEq(16L),
                        List.of("id", "geometry")),
                scenario(
                        "geo bbox intersects, mixed projection",
                        GEO_FIXTURE,
                        Pred.col("geometry").bboxIntersects(lowerHalf),
                        List.of("id", "geometry")),
                scenario(
                        "geo bbox intersects nothing, flat projection",
                        GEO_FIXTURE,
                        Pred.col("geometry").bboxIntersects(offMap),
                        List.of("id")),
                scenario(
                        "geo selective on an attribute, covering-column projection",
                        GEO_FIXTURE,
                        Pred.col("id").gtEq(16L),
                        List.of("bbox")));
    }

    private static Case scenario(String label, String fixture, Predicate predicate, List<String> projectedColumns) {
        return new Case(label, fixture, predicate, projectedColumns, OptionalInt.empty(), true);
    }

    // --- reading a case ---

    private static Path fileOf(Case testCase) {
        if (GEO_FIXTURE.equals(testCase.fixture())) {
            return geoFile;
        }
        return CorpusFixtures.parquetTestingData().resolve(testCase.fixture());
    }

    private static ReadOptions optionsOf(Case testCase) {
        ReadOptions.Builder builder = ReadOptions.builder().useColumnIndexFilter(testCase.withIndex());
        if (testCase.batchSize().isPresent()) {
            builder.batchSize(testCase.batchSize().getAsInt());
        }
        return builder.build();
    }

    /** The projection of every leaf under the case's top-level columns, in schema order. */
    private static Projection projectionOf(ParquetSchema schema, List<String> topLevelColumns) {
        List<ColumnPath> leaves = new ArrayList<>();
        List<String> resolved = new ArrayList<>();
        for (ColumnPath leaf : schema.leafColumns()) {
            String topLevel = leaf.part(0);
            if (topLevelColumns.contains(topLevel)) {
                leaves.add(leaf);
                resolved.add(topLevel);
            }
        }
        assertThat(resolved)
                .as("a projected column that names no leaf of the fixture would silently shrink the comparison")
                .containsAll(topLevelColumns);
        return Projection.ofPhysical(leaves);
    }

    private static List<ColumnPath> comparedColumns(List<String> topLevelColumns) {
        return topLevelColumns.stream().map(ColumnPath::of).toList();
    }

    private static List<List<Object>> readRows(
            ParquetFileReader reader,
            Predicate predicate,
            Projection projection,
            ReadOptions options,
            List<ColumnPath> compared) {
        try (Stream<ParquetRecord> rows = reader.read(predicate, projection, options)) {
            return rows.map(row -> valuesOf(row, compared)).toList();
        }
    }

    private static List<List<Object>> batchRows(
            ParquetFileReader reader,
            Predicate predicate,
            Projection projection,
            ReadOptions options,
            List<ColumnPath> compared) {
        List<List<Object>> rows = new ArrayList<>();
        try (Stream<ParquetRecordBatch> batches = reader.readBatches(predicate, projection, options)) {
            batches.forEach(batch -> collectRows(batch, compared, rows));
        }
        return rows;
    }

    private static void collectRows(ParquetRecordBatch batch, List<ColumnPath> compared, List<List<Object>> into) {
        try (batch) {
            for (int row = 0; row < batch.rowCount(); row++) {
                into.add(valuesOf(batch.materialize(row), compared));
            }
        }
    }

    // --- the reference: decode everything, then filter in Java ---

    /**
     * The rows {@code predicate} selects when nothing is pushed into the scan: an unfiltered decode of every column,
     * materialized row by row and tested through the record-level evaluator, keeping the compared columns of each row
     * that holds.
     */
    private static List<List<Object>> referenceRows(
            ParquetFileReader reader, Predicate predicate, List<ColumnPath> compared) {
        List<List<Object>> rows = new ArrayList<>();
        forEachMatchingRow(reader, predicate, row -> rows.add(valuesOf(row, compared)));
        return rows;
    }

    private static List<Long> referenceGeoIds(ParquetFileReader reader, Predicate predicate) {
        List<Long> ids = new ArrayList<>();
        forEachMatchingRow(reader, predicate, row -> ids.add(row.getLong(ColumnPath.of("id"))));
        return ids;
    }

    private static void forEachMatchingRow(
            ParquetFileReader reader, Predicate predicate, Consumer<ParquetRecord> action) {
        try (Stream<ParquetRecordBatch> batches =
                reader.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            batches.forEach(batch -> testEachRow(batch, predicate, action));
        }
    }

    private static void testEachRow(ParquetRecordBatch batch, Predicate predicate, Consumer<ParquetRecord> action) {
        try (batch) {
            for (int row = 0; row < batch.rowCount(); row++) {
                ParquetRecord materialized = batch.materialize(row);
                if (RecordLevelEvaluator.test(predicate, RecordAccessors.of(materialized))) {
                    action.accept(materialized);
                }
            }
        }
    }

    // --- comparable row values ---

    /** One row's compared columns as detached values, deeply equal-comparable across the two read paths. */
    private static List<Object> valuesOf(ParquetRecord materialized, List<ColumnPath> compared) {
        List<Object> values = new ArrayList<>(compared.size());
        for (ColumnPath column : compared) {
            values.add(detach(materialized.get(column)));
        }
        return values;
    }

    private static Object detach(Object value) {
        return switch (value) {
            case null -> null;
            case MemorySegment segment -> ByteBuffer.wrap(segment.toArray(JAVA_BYTE));
            case ParquetRecord nested -> detachRecord(nested);
            case Map<?, ?> map -> detachMap(map);
            case List<?> list -> detachList(list);
            default -> value;
        };
    }

    private static Map<String, Object> detachRecord(ParquetRecord materialized) {
        Map<String, Object> values = LinkedHashMap.newLinkedHashMap(materialized.columnCount());
        for (int column = 0; column < materialized.columnCount(); column++) {
            ColumnPath path = materialized.columnPath(column);
            values.put(path.dot(), detach(materialized.get(path)));
        }
        return values;
    }

    private static Map<Object, Object> detachMap(Map<?, ?> map) {
        Map<Object, Object> values = LinkedHashMap.newLinkedHashMap(map.size());
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            values.put(detach(entry.getKey()), detach(entry.getValue()));
        }
        return values;
    }

    private static List<Object> detachList(List<?> list) {
        List<Object> values = new ArrayList<>(list.size());
        for (Object element : list) {
            values.add(detach(element));
        }
        return values;
    }

    // --- the written GeoParquet fixture ---

    /**
     * Points at {@code (i, 2i)} for {@code i} in {@code [0, 24)}, eight rows per row group, with an {@code id}
     * attribute. The default geo write adds the {@code bbox} covering columns a spatial predicate lowers onto, and the
     * coordinates are whole numbers, which those float covering leaves hold exactly.
     */
    private static Path writeGeoPoints(Path tmp) throws Exception {
        ParquetSchema schema = geoSchema();
        WriteOptions options = WriteOptions.builder()
                .tempDir(tmp)
                .crsEpsg("geometry", 4326)
                .rowGroupSize(RowGroupSize.rows(8L))
                .build();
        Path file = tmp.resolve("geo-points.parquet");
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(8);
            for (int point = 0; point < GEO_POINTS; point++) {
                WriteFixtures.appendRow(appender, schema, geoRow(point));
            }
            appender.flush();
        }
        return file;
    }

    private static Map<ColumnPath, Object> geoRow(int point) {
        Map<ColumnPath, Object> row = new HashMap<>(2);
        row.put(ColumnPath.of("id"), (long) point);
        row.put(ColumnPath.of("geometry"), Wkb.fromWkt("POINT (" + xOf(point) + " " + yOf(point) + ")"));
        return row;
    }

    private static double xOf(long point) {
        return point;
    }

    private static double yOf(long point) {
        return 2.0 * point;
    }

    private static ParquetSchema geoSchema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive geometry = new SchemaNode.Primitive(
                "geometry", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id, geometry), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    /** The box of the given points, or empty when there is none, as the four ordinates the reader reports. */
    private static Optional<List<Double>> pointsBoxOf(List<Long> points) {
        if (points.isEmpty()) {
            return Optional.empty();
        }
        double minX = Double.MAX_VALUE;
        double minY = Double.MAX_VALUE;
        double maxX = -Double.MAX_VALUE;
        double maxY = -Double.MAX_VALUE;
        for (Long point : points) {
            minX = Math.min(minX, xOf(point));
            maxX = Math.max(maxX, xOf(point));
            minY = Math.min(minY, yOf(point));
            maxY = Math.max(maxY, yOf(point));
        }
        return Optional.of(List.of(minX, minY, maxX, maxY));
    }

    private static Optional<List<Double>> boxOf(Optional<BoundingBox> bounds) {
        return bounds.map(box -> List.of(box.xmin(), box.ymin(), box.xmax(), box.ymax()));
    }
}
