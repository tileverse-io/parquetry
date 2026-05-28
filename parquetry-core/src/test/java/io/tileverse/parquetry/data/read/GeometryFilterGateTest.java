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

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteRow;
import io.tileverse.parquetry.filter.GeometryFilter;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.Wkb;

/**
 * End-to-end proof that {@link GeometryFilter#gate} is called from the record-level filter and produces the correct row
 * selection. A small point fixture (8 rows, 1 row group) is written and read back with a fake point-in-box filter that
 * counts invocations of {@link GeometryFilter#decode}. The test asserts: (a) only rows whose point is inside the query
 * box are returned, and (b) {@code decode} is called exactly once per row that reached the gate - no double-decode for
 * matched rows, and the counter equals the total row count when no pruning applies.
 */
class GeometryFilterGateTest {

    private static final ColumnPath GEOM = ColumnPath.of("geometry");
    private static final ColumnPath ID = ColumnPath.of("id");

    // WKB point layout (ISO 2D little-endian): 1-byte order marker + 4-byte type int + 8-byte x + 8-byte y
    private static final int WKB_HEADER_BYTES = 5;
    private static final ValueLayout.OfDouble DOUBLE_LE =
            ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    @TempDir
    Path tempDir;

    private Path fixtureFile;

    /**
     * Eight point rows on a 4x2 grid. Points inside [2,0 - 6,2]:
     *
     * <ul>
     *   <li>id=2 at (3,1)
     *   <li>id=3 at (5,1)
     * </ul>
     *
     * Points outside the box: ids 0,1,4,5,6,7.
     */
    @BeforeEach
    void writeFixture() throws Exception {
        ParquetSchema schema = flatSchema(requiredBinary("geometry"), requiredInt32("id"));
        WriteOptions options = WriteOptions.builder().tempDir(tempDir).build();
        fixtureFile = tempDir.resolve("geomfilter.parquet");
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(fixtureFile), schema, options)) {
            writer.write(pointRow(0, 0.0, 0.0));
            writer.write(pointRow(1, 1.0, 1.0));
            writer.write(pointRow(2, 3.0, 1.0)); // inside [2,0 - 6,2]
            writer.write(pointRow(3, 5.0, 1.0)); // inside [2,0 - 6,2]
            writer.write(pointRow(4, 7.0, 1.0));
            writer.write(pointRow(5, 8.0, 3.0));
            writer.write(pointRow(6, 1.0, 3.0));
            writer.write(pointRow(7, 9.0, 9.0));
        }
    }

    @Test
    void onlyRowsInsideTheBoxAreReturned() {
        AtomicInteger decodes = new AtomicInteger();
        GeometryFilter<double[]> filter = pointInBox(2, 0, 6, 2, decodes);
        List<Integer> ids = readIds(Predicate.geometryFilter(filter));
        assertThat(ids).as("only points inside [2,0 - 6,2] should be returned").containsExactlyInAnyOrder(2, 3);
    }

    @Test
    void decodeIsCalledExactlyOncePerRowWithNoPruning() {
        // pruningPredicate() returns empty - every row reaches the gate; decode must be called exactly once per row.
        AtomicInteger decodes = new AtomicInteger();
        GeometryFilter<double[]> filter = pointInBox(2, 0, 6, 2, decodes);
        readIds(Predicate.geometryFilter(filter));
        assertThat(decodes.get())
                .as("decode must be called exactly once per row when no pruning skips rows (8 rows total)")
                .isEqualTo(8);
    }

    @Test
    void countThroughTheGateIsExactWithoutMaterializingTheGeometryOutput() {
        // Project only id: the geometry column is still decoded as a predicate input for the gate, but it is not
        // materialized as an output column. The count is exactly the number of rows the gate accepts.
        GeometryFilter<double[]> filter = pointInBox(2, 0, 6, 2, new AtomicInteger());
        long matches = count(Predicate.geometryFilter(filter), Projection.of(Set.of(ID)));
        assertThat(matches)
                .as("count through the gate equals the number of points inside the box")
                .isEqualTo(2);
    }

    @Test
    void existsThroughTheGateReflectsWhetherAnyRowPasses() {
        GeometryFilter<double[]> hasMatches = pointInBox(2, 0, 6, 2, new AtomicInteger());
        assertThat(exists(Predicate.geometryFilter(hasMatches), Projection.of(Set.of(ID))))
                .as("a filter with matching rows reports existence")
                .isTrue();

        GeometryFilter<double[]> noMatches = pointInBox(100, 100, 110, 110, new AtomicInteger());
        assertThat(exists(Predicate.geometryFilter(noMatches), Projection.of(Set.of(ID))))
                .as("a filter with no matching rows reports absence")
                .isFalse();
    }

    // --- helpers ---

    private long count(Predicate predicate, Projection projection) {
        try (ByteRangeSource src = ByteRangeSource.ofFile(fixtureFile)) {
            ParquetDataset ds = ParquetDataset.open(src);
            try (Stream<ParquetRecord> rows = ds.read(predicate, projection, ReadOptions.DEFAULTS)) {
                return rows.count();
            }
        }
    }

    private boolean exists(Predicate predicate, Projection projection) {
        try (ByteRangeSource src = ByteRangeSource.ofFile(fixtureFile)) {
            ParquetDataset ds = ParquetDataset.open(src);
            try (Stream<ParquetRecord> rows = ds.read(predicate, projection, ReadOptions.DEFAULTS)) {
                return rows.findAny().isPresent();
            }
        }
    }

    private List<Integer> readIds(Predicate predicate) {
        List<Integer> ids = new ArrayList<>();
        try (ByteRangeSource src = ByteRangeSource.ofFile(fixtureFile)) {
            ParquetDataset ds = ParquetDataset.open(src);
            try (Stream<ParquetRecord> rows = ds.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
                rows.forEach(r -> ids.add(r.getInt(ID)));
            }
        }
        return ids;
    }

    private static WriteRow pointRow(int id, double x, double y) {
        MemorySegment geom = Wkb.fromWkt("POINT (%s %s)".formatted(x, y));
        return WriteRow.of(Map.of(GEOM, geom, ID, id));
    }

    /**
     * A geometry filter that decodes WKB points (ISO 2D LE) and matches when the point is inside [minX, maxX] x [minY,
     * maxY] (edges inclusive). Each call to {@link #decode} increments {@code decodeCount}.
     */
    private static GeometryFilter<double[]> pointInBox(
            double minX, double minY, double maxX, double maxY, AtomicInteger decodeCount) {
        return new GeometryFilter<>() {
            @Override
            public ColumnPath column() {
                return GEOM;
            }

            @Override
            public Optional<Predicate.Spatial> pruningPredicate() {
                return Optional.empty();
            }

            @Override
            public double[] decode(MemorySegment wkb) {
                decodeCount.incrementAndGet();
                double x = wkb.get(DOUBLE_LE, WKB_HEADER_BYTES);
                double y = wkb.get(DOUBLE_LE, WKB_HEADER_BYTES + Double.BYTES);
                return new double[] {x, y};
            }

            @Override
            public boolean matches(double[] point) {
                return point[0] >= minX && point[0] <= maxX && point[1] >= minY && point[1] <= maxY;
            }
        };
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

    private static SchemaNode.Primitive requiredBinary(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }
}
