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
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.internal.filter.spatial.BoundsAccumulator;
import io.tileverse.parquetry.internal.filter.spatial.WkbEnvelope;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.observe.QueryObserver;
import io.tileverse.parquetry.observe.RowGroupRead;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Exercises {@link ParquetFileReader#bounds} against written GeoParquet fixtures whose native geometry statistics equal
 * the exact WKB envelopes, making a brute-force scan of the same predicate an exact oracle for the engine's answer.
 *
 * <p>The fixtures use the reader's own writer with a native geometry column, which records each row group's exact
 * bounding box (unlike a conventional {@code bbox} covering column, which is conservatively rounded and would not equal
 * the WKB oracle to the bit).
 */
class ReaderBoundsTest {

    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");
    private static final ColumnPath ID = ColumnPath.of("id");

    @TempDir
    Path tempDir;

    @Test
    void unfilteredEqualsFileMetadataAndOracle() throws IOException {
        Path file = writeGeometryFixture("unfiltered.parquet", 4L, grid(12));
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            Optional<BoundingBox> bounds = reader.bounds(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);
            BoundingBox footerBox = reader.fileStats().geometryBounds().get(GEOMETRY);
            Optional<BoundingBox> oracle = bruteForce(reader, Predicate.ALWAYS_TRUE);

            assertThat(bounds).isPresent();
            assertThat(footerBox)
                    .as("the file records a native geometry bounding box")
                    .isNotNull();
            assertSameBox2d(bounds.orElseThrow(), footerBox);
            assertSameBox2d(bounds.orElseThrow(), oracle.orElseThrow());
        }
    }

    @Test
    void attributeFilterEqualsOracle() throws IOException {
        Path file = writeGeometryFixture("attribute.parquet", 4L, grid(12));
        Predicate predicate = Pred.col("id").gtEq(5);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            Optional<BoundingBox> bounds = reader.bounds(predicate, ReadOptions.DEFAULTS);
            Optional<BoundingBox> oracle = bruteForce(reader, predicate);

            assertThat(oracle)
                    .as("the predicate must select a non-empty subset for a meaningful test")
                    .isPresent();
            assertThat(bounds).isPresent();
            assertSameBox2d(bounds.orElseThrow(), oracle.orElseThrow());
        }
    }

    @Test
    void spatialFilterEqualsOracle() throws IOException {
        Path file = writeGeometryFixture("spatial.parquet", 4L, grid(12));
        // The grid's points lie at (i, 2 * i) up to (11, 22); this window keeps only i <= 6 and clips the tail.
        Predicate predicate = Pred.col("geometry").bboxIntersects(Bbox.of2d(-1.0, -1.0, 13.0, 13.0));
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            Optional<BoundingBox> bounds = reader.bounds(predicate, ReadOptions.DEFAULTS);
            Optional<BoundingBox> oracle = bruteForce(reader, predicate);
            Optional<BoundingBox> unfiltered = reader.bounds(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);

            assertThat(oracle)
                    .as("the query window must clip a non-empty subset for a meaningful test")
                    .isPresent();
            assertThat(bounds).isPresent();
            assertSameBox2d(bounds.orElseThrow(), oracle.orElseThrow());
            assertThat(bounds.orElseThrow().xmax())
                    .as("an engine ignoring the spatial predicate would return the full extent")
                    .isLessThan(unfiltered.orElseThrow().xmax());
            assertThat(bounds.orElseThrow().ymax())
                    .isLessThan(unfiltered.orElseThrow().ymax());
        }
    }

    @Test
    void alwaysFalseIsEmpty() throws IOException {
        Path file = writeGeometryFixture("always-false.parquet", 4L, grid(12));
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            assertThat(reader.bounds(Predicate.ALWAYS_FALSE, ReadOptions.DEFAULTS))
                    .isEmpty();
        }
    }

    @Test
    void noGeometryColumnIsEmpty() throws IOException {
        Path file = writeAttributeOnlyFixture("no-geometry.parquet");
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            assertThat(reader.bounds(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                    .isEmpty();
        }
    }

    @Test
    void noMatchIsEmpty() throws IOException {
        Path file = writeGeometryFixture("no-match.parquet", 4L, grid(12));
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            assertThat(reader.bounds(Pred.col("id").gtEq(1_000_000), ReadOptions.DEFAULTS))
                    .isEmpty();
        }
    }

    @Test
    void coveredRowGroupSkipsItsDecode() throws IOException {
        Path file = writeGeometryFixture("covered.parquet", 5L, nestedRowGroups());
        // Every id is even and none equals 11: the predicate matches every row yet the statistics cannot prove it,
        // keeping both row groups in the residual rather than folding them from metadata. Should a future pruning
        // tier prove the all-match, both fold from metadata with no decode and the contains(0) assertion fails loudly.
        Predicate matchesEveryRow = Pred.col("id").notEq(11);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            RecordingObserver observer = new RecordingObserver();
            ReadOptions options = ReadOptions.builder().queryObserver(observer).build();

            Optional<BoundingBox> bounds = reader.bounds(matchesEveryRow, options);
            Optional<BoundingBox> oracle = bruteForce(reader, matchesEveryRow);

            List<Integer> readRowGroups = observer.readRowGroupIndexes();
            assertThat(readRowGroups)
                    .as("the second row group lies inside the first, whose decode seeds the accumulated bounds")
                    .doesNotContain(1)
                    .contains(0);
            assertSameBox2d(bounds.orElseThrow(), oracle.orElseThrow());
        }
    }

    // --- oracle ---

    /** The exact bounding box of every {@code predicate}-matching row, scanned row by row from the geometry WKB. */
    private static Optional<BoundingBox> bruteForce(ParquetFileReader reader, Predicate predicate) {
        BoundsAccumulator oracle = new BoundsAccumulator();
        try (Stream<ParquetRecordBatch> batches =
                reader.readBatches(predicate, Projection.ofPhysical(List.of(GEOMETRY)), ReadOptions.DEFAULTS)) {
            batches.forEach(batch -> {
                BinaryVector cells = (BinaryVector) batch.columns().get(GEOMETRY);
                for (int row = 0; row < batch.rowCount(); row++) {
                    MemorySegment wkb = cells.get(row);
                    if (wkb != null) {
                        Bbox envelope = WkbEnvelope.compute(wkb);
                        oracle.unionXy(envelope.minX(), envelope.minY(), envelope.maxX(), envelope.maxY());
                    }
                }
                batch.close();
            });
        }
        return oracle.snapshot();
    }

    private static void assertSameBox2d(BoundingBox actual, BoundingBox expected) {
        assertThat(actual.xmin()).as("xmin").isEqualTo(expected.xmin());
        assertThat(actual.ymin()).as("ymin").isEqualTo(expected.ymin());
        assertThat(actual.xmax()).as("xmax").isEqualTo(expected.xmax());
        assertThat(actual.ymax()).as("ymax").isEqualTo(expected.ymax());
    }

    // --- fixtures ---

    /** A row per point on a widening grid: id {@code i}, point {@code (i, 2*i)}; a range filter on id clips rows. */
    private static List<Feature> grid(int rows) {
        List<Feature> features = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) {
            features.add(new Feature(i, i, 2.0 * i));
        }
        return features;
    }

    /**
     * Two row groups whose extents nest: the first spans a large box, the second lies strictly inside it. Every id is
     * even, and a {@code notEq(11)} predicate matches every row without any row group's statistics proving it.
     */
    private static List<Feature> nestedRowGroups() {
        return List.of(
                new Feature(10, 0.0, 0.0),
                new Feature(12, 100.0, 0.0),
                new Feature(14, 0.0, 100.0),
                new Feature(16, 100.0, 100.0),
                new Feature(18, 50.0, 50.0),
                new Feature(10, 40.0, 40.0),
                new Feature(12, 60.0, 40.0),
                new Feature(14, 40.0, 60.0),
                new Feature(16, 60.0, 60.0),
                new Feature(18, 50.0, 50.0));
    }

    private Path writeGeometryFixture(String name, long rowGroupRows, List<Feature> features) throws IOException {
        ParquetSchema schema = flatSchema(requiredBinary("geometry"), requiredInt32("id"));
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(RowGroupSize.rows(rowGroupRows))
                .crsEpsg("geometry", 4326)
                .build();
        Path file = tempDir.resolve(name);
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(1);
            for (Feature feature : features) {
                WriteFixtures.appendRow(
                        appender,
                        schema,
                        Map.of(
                                GEOMETRY, MemorySegment.ofArray(wkbPoint(feature.x(), feature.y())),
                                ID, feature.id()));
            }
        }
        return file;
    }

    private Path writeAttributeOnlyFixture(String name) throws IOException {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options = WriteOptions.builder().tempDir(tempDir).build();
        Path file = tempDir.resolve(name);
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(1);
            for (int i = 0; i < 8; i++) {
                WriteFixtures.appendRow(appender, schema, Map.of(ID, i));
            }
        }
        return file;
    }

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children =
                Stream.of(leaves).map(leaf -> (SchemaNode) leaf).toList();
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

    private record Feature(int id, double x, double y) {}

    private static final class RecordingObserver implements QueryObserver {

        private final List<RowGroupRead> events = new ArrayList<>();

        @Override
        public synchronized void onRowGroupRead(RowGroupRead event) {
            events.add(event);
        }

        synchronized List<Integer> readRowGroupIndexes() {
            List<Integer> indexes = new ArrayList<>(events.size());
            for (RowGroupRead event : events) {
                indexes.add(event.rowGroupIndex());
            }
            return indexes;
        }
    }
}
