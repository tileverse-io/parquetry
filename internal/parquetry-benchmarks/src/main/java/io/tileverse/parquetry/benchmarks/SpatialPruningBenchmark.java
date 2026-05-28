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
package io.tileverse.parquetry.benchmarks;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.GeoParquetMetadataMode;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.data.WriteRow;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Measures the row-group pruning benefit of spatially clustered GeoParquet data.
 *
 * <p>A parquetry-written GeoParquet file has native per-row-group geometry statistics (min/max bounding boxes, no
 * covering columns). A {@code bboxIntersects} query can therefore skip row groups whose union bounding box is disjoint
 * from the query -- but only when the data is spatially clustered. When rows are shuffled, every row group's bounding
 * box covers the full extent and no row groups can be eliminated; the reader falls back to evaluating every row's WKB
 * against the query bbox.
 *
 * <p>The two {@code layout} arms isolate this:
 *
 * <ul>
 *   <li>{@code CLUSTERED} writes rows in spatial order: each row group occupies a distinct sub-tile of the grid. A
 *       query overlapping only a few tiles eliminates most row groups at the row-group-stats tier.
 *   <li>{@code SHUFFLED} randomises row order: every row group's union bbox spans the whole extent, nothing is
 *       eliminated, and every row is evaluated at the record-level tier.
 * </ul>
 *
 * <p>The {@code geometrySize} axis reveals how record-level WKB walking cost scales with polygon vertex count:
 *
 * <ul>
 *   <li>{@code SMALL} encodes a 5-point closed ring (axis-aligned rectangle) per row -- minimal WKB.
 *   <li>{@code LARGE} encodes a dense ring of ~2000 vertices per row. On the SHUFFLED arm, where record-level
 *       evaluation dominates, LARGE is significantly slower than SMALL; on the CLUSTERED arm the pruning tier skips
 *       most rows before WKB walking, narrowing the gap.
 * </ul>
 *
 * <p>The query bbox overlaps only two of the sixteen row groups in the CLUSTERED layout, keeping that arm's
 * record-level work tiny regardless of geometry size.
 *
 * <pre>{@code
 * ./mvnw -Pbenchmarks -pl :parquetry-benchmarks -am package
 * java --enable-preview --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
 *   -jar internal/parquetry-benchmarks/target/benchmarks.jar SpatialPruningBenchmark
 * }</pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class SpatialPruningBenchmark {

    private static final long SHUFFLE_SEED = 42L;

    /**
     * Number of grid tiles along each axis. The total row group count equals GRID_SIZE squared. Each tile is 1x1 degree
     * in the fixture's coordinate space.
     */
    private static final int GRID_SIZE = 4;

    private static final int FULL_ROWS_PER_GROUP = 2_000;
    private static final int SMOKE_ROWS_PER_GROUP = 50;

    /** Full vertex count for the LARGE polygon ring; dense enough to make WKB-walk cost measurable. */
    private static final int LARGE_RING_VERTICES = 2_000;
    /** Vertex count for the LARGE ring in smoke mode; exercises the same code path with much less data. */
    private static final int SMOKE_RING_VERTICES = 64;

    /**
     * Spatial ordering of rows on disk. CLUSTERED writes one row group per grid tile; SHUFFLED randomises row order so
     * every row group's union bbox spans the whole extent.
     */
    public enum Layout {
        CLUSTERED,
        SHUFFLED
    }

    /**
     * Per-row geometry complexity. SMALL is a 5-point axis-aligned rectangle (minimal WKB). LARGE is a dense polygon
     * ring whose vertex count is controlled by {@code LARGE_RING_VERTICES} / {@code SMOKE_RING_VERTICES}.
     */
    public enum GeometrySize {
        SMALL,
        LARGE
    }

    @Param({"CLUSTERED", "SHUFFLED"})
    private Layout layout;

    @Param({"SMALL", "LARGE"})
    private GeometrySize geometrySize;

    /**
     * Shrinks the fixture to a few hundred rows per row group for the CI sanity run that only checks the benchmark
     * executes. The default keeps the full measurement workload.
     */
    @Param({"false"})
    private boolean smoke;

    private Path workDir;
    private SyntheticParquet.OpenDataset open;
    private Predicate predicate;

    @SuppressWarnings("java:S125") // comment above explains the fixture query intent, not commented-out code
    @Setup(Level.Trial)
    public void setUp() throws IOException {
        int rowsPerGroup = smoke ? SMOKE_ROWS_PER_GROUP : FULL_ROWS_PER_GROUP;
        int ringVertices = smoke ? SMOKE_RING_VERTICES : LARGE_RING_VERTICES;

        workDir = Files.createTempDirectory("parquetry-bench-spatial-pruning-");
        Path file = workDir.resolve("spatial-pruning.parquet");
        writeFixture(file, rowsPerGroup, ringVertices);
        open = SyntheticParquet.open(file);

        // The query overlaps only the two grid tiles at (0,0)-(1,1) and (0,1)-(1,2).
        // In the CLUSTERED layout exactly two row groups survive the row-group-stats tier;
        // in the SHUFFLED layout all sixteen row groups survive and every row is evaluated.
        predicate = Pred.col(GEOMETRY_NAME).bboxIntersects(Bbox.of2d(0.0, 0.0, 1.0, 2.0));
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        open.close();
        SyntheticParquet.deleteRecursively(workDir);
    }

    /** Reads all rows matching the query bbox and consumes each row's {@code id} through the blackhole. */
    @Benchmark
    public void bboxIntersectsRead(Blackhole bh) {
        try (Stream<ParquetRecord> rows = open.dataset().read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            rows.forEach(row -> bh.consume(row.getInt(ID_COL)));
        }
    }

    // --- fixture writing ---

    private void writeFixture(Path file, int rowsPerGroup, int ringVertices) throws IOException {
        ParquetSchema schema = buildSchema();
        WriteOptions options = WriteOptions.builder()
                .tempDir(workDir)
                .geoParquetMetadata(GeoParquetMetadataMode.V2_0_ONLY)
                .crsEpsg(GEOMETRY_NAME, 4326)
                .rowGroupSize(RowGroupSize.rows(rowsPerGroup))
                .encodingPolicy("id", WriteOptions.EncodingPolicy.FORCE_PLAIN)
                .encodingPolicy(GEOMETRY_NAME, WriteOptions.EncodingPolicy.FORCE_PLAIN)
                .build();

        List<GeometryRow> rows = buildRows(rowsPerGroup, ringVertices);
        if (layout == Layout.SHUFFLED) {
            @SuppressWarnings("java:S2245") // reproducible shuffle, not security-sensitive
            Random random = new Random(SHUFFLE_SEED);
            Collections.shuffle(rows, random);
        }

        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            for (GeometryRow row : rows) {
                writer.write(row);
            }
        }
    }

    /**
     * Builds one row per (tile, rowWithinTile) combination. The CLUSTERED layout writes them in tile order, placing
     * each tile's rows consecutively so the writer closes each row group before moving to the next tile. The SHUFFLED
     * arm shuffles this list before writing.
     */
    private List<GeometryRow> buildRows(int rowsPerGroup, int ringVertices) {
        int totalGroups = GRID_SIZE * GRID_SIZE;
        int totalRows = totalGroups * rowsPerGroup;
        List<GeometryRow> rows = new ArrayList<>(totalRows);
        int id = 0;
        for (int tileY = 0; tileY < GRID_SIZE; tileY++) {
            for (int tileX = 0; tileX < GRID_SIZE; tileX++) {
                // Each tile occupies a 1x1 degree cell; row geometries are placed inside that cell.
                double originX = tileX;
                double originY = tileY;
                for (int i = 0; i < rowsPerGroup; i++) {
                    // Spread rows within the tile uniformly; step size keeps all points inside the cell.
                    double step = 1.0 / rowsPerGroup;
                    double cx = originX + i * step;
                    double cy = originY + i * step;
                    byte[] wkb = buildWkb(cx, cy, ringVertices);
                    MemorySegment geom = MemorySegment.ofArray(wkb).asReadOnly();
                    rows.add(new GeometryRow(id, geom));
                    id++;
                }
            }
        }
        return rows;
    }

    /**
     * Builds WKB for the chosen geometry size. SMALL encodes a 5-point rectangle around the center point; LARGE encodes
     * a dense ring approximating a circle, exercising the WKB-walk cost at record-level evaluation.
     */
    private byte[] buildWkb(double cx, double cy, int ringVertices) {
        if (geometrySize == GeometrySize.SMALL) {
            double half = 0.001;
            return wkbRect(cx - half, cy - half, cx + half, cy + half);
        }
        return wkbDenseRing(cx, cy, 0.001, ringVertices);
    }

    // --- WKB encoders ---

    /**
     * Little-endian WKB Polygon: one ring of five closed points tracing the axis-aligned rectangle. The ring visits
     * corners counter-clockwise: SW -> SE -> NE -> NW -> SW.
     */
    private static byte[] wkbRect(double minX, double minY, double maxX, double maxY) {
        // 1 (byte-order) + 4 (type) + 4 (numRings) + 4 (numPoints) + 5*16 (coordinates)
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 4 + 4 + 5 * 16).order(ByteOrder.LITTLE_ENDIAN);
        buf.put((byte) 0x01);
        buf.putInt(3);
        buf.putInt(1);
        buf.putInt(5);
        double[][] ring = {{minX, minY}, {maxX, minY}, {maxX, maxY}, {minX, maxY}, {minX, minY}};
        for (double[] xy : ring) {
            buf.putDouble(xy[0]);
            buf.putDouble(xy[1]);
        }
        return buf.array();
    }

    /**
     * Little-endian WKB Polygon: one ring of {@code n} vertices approximating a circle of the given radius centred at
     * (cx, cy), closed by repeating the first vertex. This exercises the WKB-walking cost at record-level evaluation
     * when the geometry has many vertices.
     */
    private static byte[] wkbDenseRing(double cx, double cy, double radius, int n) {
        int points = n + 1; // n unique vertices plus the closing repeat
        // 1 (byte-order) + 4 (type) + 4 (numRings) + 4 (numPoints) + points*16 (coordinates)
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 4 + 4 + points * 16).order(ByteOrder.LITTLE_ENDIAN);
        buf.put((byte) 0x01);
        buf.putInt(3);
        buf.putInt(1);
        buf.putInt(points);
        for (int i = 0; i < n; i++) {
            double angle = 2.0 * Math.PI * i / n;
            buf.putDouble(cx + radius * Math.cos(angle));
            buf.putDouble(cy + radius * Math.sin(angle));
        }
        // close the ring
        double firstAngle = 0.0;
        buf.putDouble(cx + radius * Math.cos(firstAngle));
        buf.putDouble(cy + radius * Math.sin(firstAngle));
        return buf.array();
    }

    // --- schema ---

    private static ParquetSchema buildSchema() {
        SchemaNode.Primitive geomLeaf = new SchemaNode.Primitive(
                GEOMETRY_NAME,
                Repetition.REQUIRED,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.empty(),
                -1);
        SchemaNode.Primitive idLeaf = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(geomLeaf, idLeaf), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    // --- WriteRow adapter ---

    private static final String GEOMETRY_NAME = "geometry";
    private static final ColumnPath GEOMETRY_COL = ColumnPath.of(GEOMETRY_NAME);
    private static final ColumnPath ID_COL = ColumnPath.of("id");

    /** Reused across the write loop; holds one row's id and WKB geometry without allocating a map per row. */
    private static final class GeometryRow implements WriteRow {
        private final int id;
        private final MemorySegment geom;

        GeometryRow(int id, MemorySegment geom) {
            this.id = id;
            this.geom = geom;
        }

        @Override
        public Object value(ColumnPath path) {
            if (path.equals(GEOMETRY_COL)) {
                return geom;
            }
            return id;
        }
    }
}
