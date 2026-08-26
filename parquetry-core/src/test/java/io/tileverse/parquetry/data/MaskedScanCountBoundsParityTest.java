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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.SortedLongPositionSet;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.internal.filter.spatial.BoundsAccumulator;
import io.tileverse.parquetry.internal.filter.spatial.WkbEnvelope;
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
 * {@code count} and {@code bounds} select the same rows a {@code read} of the same predicate selects.
 *
 * <p>The three entry points share one decode path, and a row a read returns must be counted by {@code count} and folded
 * into the box {@code bounds} returns. The reference is the read itself: {@code count} is compared against the rows a
 * read of the same predicate yields, and {@code bounds} against the envelope union of those rows' geometry cells.
 */
class MaskedScanCountBoundsParityTest {

    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");
    private static final ColumnPath ID = ColumnPath.of("id");
    // The caller names the synthesized row-position column; the engine mandates no fixed name (Iceberg uses _pos).
    private static final ColumnPath POS = ColumnPath.of("_pos");

    private static final int FLAT_ROWS = 4_000;
    private static final int GEOMETRY_ROWS = 400;

    @TempDir
    Path tempDir;

    /** Predicates over the flat fixture's {@code year} / {@code country} / {@code value} columns. */
    static Stream<Arguments> flatPredicates() {
        return Stream.of(
                Arguments.of("one year", Pred.col("year").eq(2021)),
                Arguments.of("a whole row group's worth", Pred.col("country").eq("AR")),
                Arguments.of("no row matches", Pred.col("year").gt(9_999)),
                Arguments.of("every row matches", Pred.col("year").gtEq(2020)),
                Arguments.of(
                        "two columns",
                        Pred.and(Pred.col("year").gtEq(2021), Pred.col("value").lt(1_000.0))),
                Arguments.of(
                        "a disjunction",
                        Pred.or(Pred.col("year").eq(2020), Pred.col("value").gt(5_000.0))),
                Arguments.of(
                        "a positional delete", deletePositions(Pred.col("year").gtEq(2021), 0L, 7L, FLAT_ROWS - 1L)),
                Arguments.of("a positional delete alone", deletePositions(Predicate.ALWAYS_TRUE, 1L, 2L, 3L)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("flatPredicates")
    void countEqualsTheRowsAReadReturns(String label, Predicate predicate) throws IOException {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tempDir, FLAT_ROWS);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            long counted = reader.count(predicate, ReadOptions.DEFAULTS);
            long read = readRowCount(reader, predicate);

            assertThat(counted).as("%s: count agrees with read", label).isEqualTo(read);
        }
    }

    /** Predicates over the geometry fixture, each selecting a different slice of the grid. */
    static Stream<Arguments> geometryPredicates() {
        return Stream.of(
                Arguments.of("a low id prefix", Pred.col("id").lt(20)),
                Arguments.of(
                        "an interior id band",
                        Pred.and(Pred.col("id").gtEq(100), Pred.col("id").lt(140))),
                Arguments.of("no row matches", Pred.col("id").gt(100_000)),
                Arguments.of("every row matches", Pred.col("id").gtEq(0)),
                Arguments.of("a bbox window", Pred.col("geometry").bboxIntersects(Bbox.of2d(-1.0, -1.0, 50.0, 50.0))),
                Arguments.of(
                        "a positional delete", deletePositions(Pred.col("id").gtEq(0), 0L, GEOMETRY_ROWS - 1L)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("geometryPredicates")
    void boundsEqualTheEnvelopeOfTheRowsAReadReturns(String label, Predicate predicate) throws IOException {
        Path file = writeGeometryFixture("bounds-parity.parquet");
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            Optional<BoundingBox> bounds = reader.bounds(predicate, ReadOptions.DEFAULTS);
            Optional<BoundingBox> expected = foldReadGeometries(reader, predicate);

            assertThat(bounds.isPresent())
                    .as("%s: bounds and the read agree on whether any row matches", label)
                    .isEqualTo(expected.isPresent());
            if (expected.isPresent()) {
                assertSameBox2d(label, bounds.orElseThrow(), expected.orElseThrow());
            }
        }
    }

    @Test
    void countAndBoundsAgreeOnTheSameSelectiveGeometryPredicate() throws IOException {
        Path file = writeGeometryFixture("count-bounds-agreement.parquet");
        Predicate predicate = Pred.col("id").lt(20);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            long counted = reader.count(predicate, ReadOptions.DEFAULTS);
            Optional<BoundingBox> bounds = reader.bounds(predicate, ReadOptions.DEFAULTS);

            assertThat(counted).as("twenty ids below 20 match").isEqualTo(20L);
            assertThat(bounds).isPresent();
            assertSameBox2d("selective", bounds.orElseThrow(), boxOf(0.0, 0.0, 19.0, 38.0));
        }
    }

    @Test
    void boundsOverARowPositionOnlyPredicateEqualTheRead() throws IOException {
        Path file = writeGeometryFixture("bounds-row-position-only.parquet");
        // The grid's corners sit at the first and last positions: excluding both must shrink the box on every side.
        Predicate predicate = deletePositions(Predicate.ALWAYS_TRUE, 0L, GEOMETRY_ROWS - 1L);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            Optional<BoundingBox> bounds = reader.bounds(predicate, ReadOptions.DEFAULTS);
            Optional<BoundingBox> expected = foldReadGeometries(reader, predicate);
            Optional<BoundingBox> unfiltered = reader.bounds(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);

            assertThat(bounds).isPresent();
            assertSameBox2d("row positions only", bounds.orElseThrow(), expected.orElseThrow());
            assertThat(bounds.orElseThrow().xmax())
                    .as("dropping the max-coordinate row shrinks the box")
                    .isLessThan(unfiltered.orElseThrow().xmax());
        }
    }

    // --- oracles ---

    /** The rows a read of {@code predicate} returns, counted one by one. */
    private static long readRowCount(ParquetFileReader reader, Predicate predicate) {
        try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.count();
        }
    }

    /** The envelope union of the geometry cell of every row a read of {@code predicate} returns. */
    private static Optional<BoundingBox> foldReadGeometries(ParquetFileReader reader, Predicate predicate) {
        BoundsAccumulator accumulator = new BoundsAccumulator();
        Projection geometryOnly = Projection.ofPhysical(List.of(GEOMETRY));
        try (Stream<ParquetRecordBatch> batches = reader.readBatches(predicate, geometryOnly, ReadOptions.DEFAULTS)) {
            batches.forEach(batch -> {
                foldBatch(batch, accumulator);
                batch.close();
            });
        }
        return accumulator.snapshot();
    }

    private static void foldBatch(ParquetRecordBatch batch, BoundsAccumulator accumulator) {
        BinaryVector cells = (BinaryVector) batch.columns().get(GEOMETRY);
        for (int row = 0; row < batch.rowCount(); row++) {
            MemorySegment wkb = cells.get(row);
            if (wkb != null) {
                Bbox envelope = WkbEnvelope.compute(wkb);
                accumulator.unionXy(envelope.minX(), envelope.minY(), envelope.maxX(), envelope.maxY());
            }
        }
    }

    private static void assertSameBox2d(String label, BoundingBox actual, BoundingBox expected) {
        assertThat(actual.xmin()).as("%s: xmin", label).isEqualTo(expected.xmin());
        assertThat(actual.ymin()).as("%s: ymin", label).isEqualTo(expected.ymin());
        assertThat(actual.xmax()).as("%s: xmax", label).isEqualTo(expected.xmax());
        assertThat(actual.ymax()).as("%s: ymax", label).isEqualTo(expected.ymax());
    }

    private static BoundingBox boxOf(double xmin, double ymin, double xmax, double ymax) {
        OptionalDouble absent = OptionalDouble.empty();
        return new BoundingBox(xmin, xmax, ymin, ymax, absent, absent, absent, absent);
    }

    // --- predicates ---

    /** {@code base} narrowed by a positional delete over {@code positions}, the Iceberg merge-on-read shape. */
    private static Predicate deletePositions(Predicate base, long... positions) {
        Predicate delete = new Predicate.RowIndexExcluded(POS, SortedLongPositionSet.of(positions));
        if (base instanceof Predicate.Always(boolean value) && value) {
            return delete;
        }
        return Pred.and(base, delete);
    }

    // --- fixtures ---

    /**
     * A GeoParquet file of {@link #GEOMETRY_ROWS} points on a widening grid, row {@code i} holding id {@code i} at
     * {@code (i, 2 * i)}. Its small row groups and small pages leave most pages and row groups without a surviving row
     * under a selective predicate.
     */
    private Path writeGeometryFixture(String name) throws IOException {
        ParquetSchema schema = flatSchema(requiredBinary("geometry"), requiredInt32("id"));
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(RowGroupSize.rows(50L))
                .pageValueLimit(10)
                .crsEpsg("geometry", 4326)
                .build();
        Path file = tempDir.resolve(name);
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(1);
            for (int i = 0; i < GEOMETRY_ROWS; i++) {
                Map<ColumnPath, Object> row = Map.of(GEOMETRY, MemorySegment.ofArray(wkbPoint(i, 2.0 * i)), ID, i);
                WriteFixtures.appendRow(appender, schema, row);
            }
        }
        return file;
    }

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children = new ArrayList<>(List.of(leaves));
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredInt32(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive requiredBinary(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static byte[] wkbPoint(double x, double y) {
        ByteBuffer buffer = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1);
        buffer.putInt(1);
        buffer.putDouble(x);
        buffer.putDouble(y);
        return buffer.array();
    }
}
