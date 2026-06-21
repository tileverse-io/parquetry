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
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
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

import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.GeoParquetMetadataMode;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.geo.jts.JtsGeometryFilter;
import io.tileverse.parquetry.geo.jts.JtsMaterializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Compares three query strategies for a spatial filter backed by the real JTS geometry engine.
 *
 * <p>The query polygon is a diamond (axis-aligned square rotated 45 degrees) centred at the origin of the data grid.
 * Its bounding box is strictly larger than the polygon itself: many rows fall inside the bbox but outside the diamond.
 * This ensures the three strategies return meaningfully different result sets and decode costs.
 *
 * <p>The three strategies ({@code mode} parameter):
 *
 * <ul>
 *   <li>{@code BBOX_ONLY}: reads with {@code Predicate.Spatial.BboxIntersects} using the query diamond's envelope. All
 *       rows whose geometry bbox overlaps the query envelope are returned, including rows whose geometry is outside the
 *       diamond but inside its bounding box. This is the coarse superset a caller would have to re-filter.
 *   <li>{@code IN_CORE_GATE}: reads with {@code Predicate.geometryFilter(JtsGeometryFilter.intersects(...))}. The
 *       reader's pruning tiers eliminate row groups and pages whose bbox is disjoint from the query envelope; the JTS
 *       gate then tests each surviving row's decoded WKB exactly. Only rows whose geometry truly intersects the diamond
 *       materialise their other columns. This avoids materialising the non-geometry columns of rows the exact test
 *       rejects.
 *   <li>{@code APP_SIDE_FILTER}: reads with {@code BBOX_ONLY} (full materialization of every bbox candidate), then
 *       applies the same JTS exact test app-side in a stream filter step. Models an application that pushes only the
 *       envelope to the reader and re-checks exactness after materialization. Every bbox-candidate row decodes all its
 *       output columns before the exact test discards it.
 * </ul>
 *
 * <p>{@code IN_CORE_GATE} and {@code APP_SIDE_FILTER} produce the same exact rows; {@code BBOX_ONLY} returns a superset
 * because it retains bbox-candidates that the diamond rejects.
 *
 * <p>The {@code layout} parameter controls spatial data ordering:
 *
 * <ul>
 *   <li>{@code CLUSTERED}: rows written in spatial tile order; row-group bounding boxes cover compact sub-tiles. Row
 *       groups outside the query diamond's envelope are eliminated before any record-level work.
 *   <li>{@code SHUFFLED}: row order randomised; every row group's bbox spans the full extent. No row groups are
 *       eliminated; record-level work dominates.
 * </ul>
 *
 * <p>The {@code selectivity} parameter controls the query diamond's size relative to the data grid:
 *
 * <ul>
 *   <li>{@code HIGH} selectivity: small diamond covering roughly 1/4 of the grid area. Few rows intersect; the exact
 *       filter rejects most bbox-candidates. The cost gap between BBOX_ONLY and IN_CORE_GATE is largest here.
 *   <li>{@code LOW} selectivity: large diamond covering most of the grid area. Many rows intersect; the exact test
 *       rejects few bbox-candidates. The three strategies converge.
 * </ul>
 *
 * <p>The {@code geometrySize} parameter scales per-row WKB decode cost (same axis as {@code SpatialPruningBenchmark}).
 *
 * <p>The {@code output} parameter selects how the geometry output column is materialised: WKB keeps it as raw bytes (no
 * JTS parse on the output side); JTS parses it into a {@code Geometry}. On the IN_CORE_GATE mode the JTS-minus-WKB
 * difference isolates the surviving rows' output parse - the parse the gate already did once for its exact test.
 * Reusing the gate's decoded geometry as the output value would reclaim that parse.
 *
 * <pre>{@code
 * ./mvnw -Pbenchmarks -pl :parquetry-benchmarks -am package
 * java --enable-preview --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
 *   -jar internal/parquetry-benchmarks/target/benchmarks.jar JtsSpatialFilterBenchmark
 * }</pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class JtsSpatialFilterBenchmark {

    private static final long SHUFFLE_SEED = 42L;

    /**
     * Grid size along each axis. The fixture writes GRID_SIZE^2 row groups, each occupying a 1x1 degree tile. The full
     * grid covers [0, GRID_SIZE] x [0, GRID_SIZE] degrees.
     */
    private static final int GRID_SIZE = 4;

    private static final int FULL_ROWS_PER_GROUP = 2_000;
    private static final int SMOKE_ROWS_PER_GROUP = 50;

    /** Full vertex count for the LARGE polygon ring. */
    private static final int LARGE_RING_VERTICES = 2_000;
    /** Vertex count for the LARGE ring in smoke mode. */
    private static final int SMOKE_RING_VERTICES = 64;

    /**
     * The query diamond is a square rotated 45 degrees (a rhombus). Its four vertices are placed at the cardinal
     * directions of a circle centred at (cx, cy) with a given half-diagonal. The corners are: top (cx, cy+r), right
     * (cx+r, cy), bottom (cx, cy-r), left (cx-r, cy). Its bounding box is [cx-r, cy-r, cx+r, cy+r], which is strictly
     * larger than the diamond: a row at the bbox corner is outside the diamond and will be rejected by the exact JTS
     * test, making the exact filter non-trivial.
     *
     * <p>The diamond is centred at (2.0, 2.0) -- the intersection of four tiles in the CLUSTERED layout -- to maximise
     * the contrast between row groups that survive bbox pruning and those that do not.
     */
    private static final double DIAMOND_CX = 2.0;

    private static final double DIAMOND_CY = 2.0;

    /**
     * HIGH selectivity: small diamond, half-diagonal = 1.0. Its bbox covers [1.0, 1.0, 3.0, 3.0] -- 4 of 16 tiles. The
     * exact diamond rejects the rows in the bbox corners.
     */
    private static final double HIGH_SELECTIVITY_HALF_DIAGONAL = 1.0;

    /**
     * LOW selectivity: large diamond, half-diagonal = 2.5. Its bbox covers [-0.5, -0.5, 4.5, 4.5] -- nearly all of the
     * grid. The diamond rejects only the far corners; most rows pass the exact test.
     */
    private static final double LOW_SELECTIVITY_HALF_DIAGONAL = 2.5;

    /** Column names. */
    private static final String GEOMETRY_NAME = "geometry";

    private static final ColumnPath GEOMETRY_COL = ColumnPath.of(GEOMETRY_NAME);
    private static final ColumnPath ID_COL = ColumnPath.of("id");

    // --- benchmark parameters ---

    /**
     * Spatial ordering of rows on disk. CLUSTERED writes one row group per grid tile; SHUFFLED randomises row order,
     * making every row group's bbox span the full extent.
     */
    public enum Layout {
        CLUSTERED,
        SHUFFLED
    }

    /**
     * Query diamond size relative to the data grid.
     *
     * <ul>
     *   <li>HIGH selectivity: small diamond selecting roughly 1/4 of the grid. The exact filter rejects many
     *       bbox-candidates.
     *   <li>LOW selectivity: large diamond covering most of the grid. Few rows are rejected by the exact filter.
     * </ul>
     */
    public enum Selectivity {
        HIGH,
        LOW
    }

    /**
     * Per-row geometry complexity. SMALL is a 5-point axis-aligned rectangle (minimal WKB). LARGE is a dense polygon
     * ring whose vertex count is controlled by {@code LARGE_RING_VERTICES} / {@code SMOKE_RING_VERTICES}.
     */
    public enum GeometrySize {
        SMALL,
        LARGE
    }

    /**
     * Which of the three query strategies to use.
     *
     * <ul>
     *   <li>BBOX_ONLY: coarse envelope filter only; returns a superset of the exact result.
     *   <li>IN_CORE_GATE: exact JTS test applied inside the reader; only truly matching rows materialise their columns.
     *   <li>APP_SIDE_FILTER: BBOX_ONLY read with full materialization, then exact JTS test applied in the app-side
     *       stream.
     * </ul>
     */
    public enum Mode {
        BBOX_ONLY,
        IN_CORE_GATE,
        APP_SIDE_FILTER
    }

    /**
     * How the geometry output column is materialised.
     *
     * <ul>
     *   <li>WKB: the default record materializer keeps the geometry as raw WKB; no JTS parse on the output side.
     *   <li>JTS: {@link JtsMaterializer} parses each output row's WKB into a JTS {@code Geometry}.
     * </ul>
     *
     * The interesting cell is IN_CORE_GATE: the JTS-minus-WKB difference is the surviving rows' output parse, which the
     * gate already paid once for its exact test. That difference is the upper bound on what reusing the gate's decoded
     * geometry as the output value would save.
     */
    public enum OutputForm {
        WKB,
        JTS
    }

    @Param({"BBOX_ONLY", "IN_CORE_GATE", "APP_SIDE_FILTER"})
    private Mode mode;

    @Param({"CLUSTERED", "SHUFFLED"})
    private Layout layout;

    @Param({"HIGH", "LOW"})
    private Selectivity selectivity;

    @Param({"SMALL", "LARGE"})
    private GeometrySize geometrySize;

    @Param({"WKB", "JTS"})
    private OutputForm output;

    /** Shrinks the fixture for a fast end-to-end check. The default keeps the full measurement workload. */
    @Param({"false"})
    private boolean smoke;

    // --- state ---

    private Path workDir;
    private SyntheticParquet.OpenDataset open;

    /** Predicate for BBOX_ONLY and APP_SIDE_FILTER: the query diamond's envelope. */
    private Predicate bboxPredicate;

    /** Predicate for IN_CORE_GATE: JtsGeometryFilter.intersects wrapping the diamond polygon. */
    private Predicate inCoreGatePredicate;

    /**
     * Filter instance reused in the APP_SIDE_FILTER stream step: decodes and tests each WKB against the diamond.
     * JtsGeometryFilter is thread-safe after construction; allocating one per trial is safe.
     */
    private JtsGeometryFilter appSideFilter;

    /**
     * Output materializer for the JTS {@link OutputForm}: parses each output row's geometry WKB into a JTS geometry.
     */
    private JtsMaterializer jtsMaterializer;

    @SuppressWarnings("java:S125") // explanatory comment for the fixture intent, not commented-out code
    @Setup(Level.Trial)
    public void setUp() throws IOException {
        int rowsPerGroup = smoke ? SMOKE_ROWS_PER_GROUP : FULL_ROWS_PER_GROUP;
        int ringVertices = smoke ? SMOKE_RING_VERTICES : LARGE_RING_VERTICES;

        workDir = Files.createTempDirectory("parquetry-bench-jts-spatial-filter-");
        Path file = workDir.resolve("jts-spatial-filter.parquet");
        writeFixture(file, rowsPerGroup, ringVertices);
        open = SyntheticParquet.open(file);

        double halfDiagonal =
                (selectivity == Selectivity.HIGH) ? HIGH_SELECTIVITY_HALF_DIAGONAL : LOW_SELECTIVITY_HALF_DIAGONAL;
        Geometry queryDiamond = buildDiamond(DIAMOND_CX, DIAMOND_CY, halfDiagonal);

        // BBOX_ONLY uses the diamond's envelope as a Predicate.Spatial.
        Bbox queryEnvelope = envelopeOf(queryDiamond);
        bboxPredicate = new Predicate.Spatial.BboxIntersects(GEOMETRY_COL, queryEnvelope);

        // IN_CORE_GATE wraps the real JTS intersects filter.
        inCoreGatePredicate = Predicate.geometryFilter(JtsGeometryFilter.intersects(GEOMETRY_COL, queryDiamond));

        // APP_SIDE_FILTER reuses a separate JtsGeometryFilter for the app-side stream step.
        appSideFilter = JtsGeometryFilter.intersects(GEOMETRY_COL, queryDiamond);

        // JTS output form parses each output row's geometry; Projection.ALL materializes the full schema.
        jtsMaterializer = new JtsMaterializer(open.reader().schema());
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        open.close();
        SyntheticParquet.deleteRecursively(workDir);
    }

    /**
     * Runs the query using the strategy named by {@code mode} and consumes each surviving row's {@code id} through the
     * blackhole.
     */
    @Benchmark
    public void query(Blackhole bh) {
        switch (mode) {
            case BBOX_ONLY -> runBboxOnly(bh);
            case IN_CORE_GATE -> runInCoreGate(bh);
            case APP_SIDE_FILTER -> runAppSideFilter(bh);
        }
    }

    /**
     * Reads all bbox-candidates: rows whose geometry bounding box overlaps the query envelope. This includes rows whose
     * exact geometry falls outside the diamond but inside its envelope. Returns a superset of the exact result.
     */
    private void runBboxOnly(Blackhole bh) {
        readAndConsume(bboxPredicate, bh);
    }

    /**
     * Reads with the JTS exact filter pushed into the read pipeline. Row groups and pages whose bbox is disjoint from
     * the query envelope are pruned first; the JTS gate then rejects rows inside the envelope but outside the diamond
     * before their other columns are materialised. Only truly intersecting rows contribute to the output.
     */
    private void runInCoreGate(Blackhole bh) {
        readAndConsume(inCoreGatePredicate, bh);
    }

    /**
     * Reads {@code predicate}'s result set and consumes each row, materialising the geometry output as raw WKB or as a
     * JTS geometry per the {@code output} axis.
     */
    private void readAndConsume(Predicate predicate, Blackhole bh) {
        if (output == OutputForm.JTS) {
            try (Stream<Map<ColumnPath, Object>> rows =
                    open.reader().read(predicate, Projection.ALL, jtsMaterializer, ReadOptions.DEFAULTS)) {
                rows.forEach(row -> consumeJts(row, bh));
            }
        } else {
            try (Stream<ParquetRecord> rows = open.reader().read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
                rows.forEach(row -> bh.consume(row.getInt(ID_COL)));
            }
        }
    }

    private void consumeJts(Map<ColumnPath, Object> row, Blackhole bh) {
        bh.consume(row.get(GEOMETRY_COL));
        bh.consume(row.get(ID_COL));
    }

    /**
     * Reads all bbox-candidates with full materialization (same as BBOX_ONLY), then applies the JTS exact test
     * app-side. Every bbox-candidate row decodes all its output columns, including those the exact test will discard.
     * This models an application that pushes only the envelope to the reader and re-applies exactness afterwards.
     *
     * <p>Under the WKB output form the geometry is read from the record via
     * {@link ParquetRecord#getGeometryBytes(ColumnPath)} and re-parsed for the test. Under the JTS output form the
     * geometry is already materialised as a JTS object, and the app-side test runs on that materialised geometry
     * without a second parse.
     */
    private void runAppSideFilter(Blackhole bh) {
        if (output == OutputForm.JTS) {
            try (Stream<Map<ColumnPath, Object>> rows =
                    open.reader().read(bboxPredicate, Projection.ALL, jtsMaterializer, ReadOptions.DEFAULTS)) {
                rows.filter(row -> appSideFilter.matches((Geometry) row.get(GEOMETRY_COL)))
                        .forEach(row -> consumeJts(row, bh));
            }
        } else {
            try (Stream<ParquetRecord> rows = open.reader().read(bboxPredicate, Projection.ALL, ReadOptions.DEFAULTS)) {
                rows.filter(this::appSideMatchesGeometry).forEach(row -> bh.consume(row.getInt(ID_COL)));
            }
        }
    }

    /**
     * Tests whether the geometry column of {@code row} passes the app-side exact filter. Extracts the raw WKB bytes,
     * wraps them in a read-only {@link MemorySegment}, and delegates to {@link JtsGeometryFilter#gate}.
     */
    private boolean appSideMatchesGeometry(ParquetRecord row) {
        byte[] wkbBytes = row.getBinary(GEOMETRY_COL);
        MemorySegment wkb = MemorySegment.ofArray(wkbBytes).asReadOnly();
        return appSideFilter.gate(wkb).isPresent();
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
            ParquetRecordBatchBuilder appender = writer.appender();
            for (GeometryRow row : rows) {
                appender.setBinary(0, row.geom());
                appender.setInt(1, row.id());
                appender.endRow();
            }
            appender.flush();
        }
    }

    /**
     * Builds one row per (tile, rowWithinTile) combination. Each tile covers a 1x1 degree cell; row geometries are
     * placed inside that cell. The CLUSTERED layout writes them in tile order; the SHUFFLED arm shuffles the list
     * before writing.
     */
    private List<GeometryRow> buildRows(int rowsPerGroup, int ringVertices) {
        int totalGroups = GRID_SIZE * GRID_SIZE;
        int totalRows = totalGroups * rowsPerGroup;
        List<GeometryRow> rows = new ArrayList<>(totalRows);
        int id = 0;
        for (int tileY = 0; tileY < GRID_SIZE; tileY++) {
            for (int tileX = 0; tileX < GRID_SIZE; tileX++) {
                double originX = tileX;
                double originY = tileY;
                for (int i = 0; i < rowsPerGroup; i++) {
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
     * Builds WKB for the row at (cx, cy). SMALL encodes a 5-point axis-aligned rectangle; LARGE encodes a dense ring
     * approximating a circle, exercising WKB-walk cost at record-level evaluation.
     */
    private byte[] buildWkb(double cx, double cy, int ringVertices) {
        if (geometrySize == GeometrySize.SMALL) {
            double half = 0.001;
            return wkbRect(cx - half, cy - half, cx + half, cy + half);
        }
        return wkbDenseRing(cx, cy, 0.001, ringVertices);
    }

    // --- query polygon builder ---

    /**
     * Builds a diamond polygon (axis-aligned square rotated 45 degrees) centred at (cx, cy) with the given
     * half-diagonal. The four vertices are at the four cardinal directions of the centre, at distance {@code r}. This
     * polygon's bounding box is [cx-r, cy-r, cx+r, cy+r], which strictly over-selects: points near the corners of the
     * bbox but outside the diamond are rejected by the exact JTS test.
     */
    private static Geometry buildDiamond(double cx, double cy, double r) {
        GeometryFactory factory = new GeometryFactory();
        Coordinate top = new Coordinate(cx, cy + r);
        Coordinate right = new Coordinate(cx + r, cy);
        Coordinate bottom = new Coordinate(cx, cy - r);
        Coordinate left = new Coordinate(cx - r, cy);
        Coordinate[] ring = {top, right, bottom, left, top};
        return factory.createPolygon(ring);
    }

    /** Extracts the query geometry's 2D envelope as a {@link Bbox} for the BBOX_ONLY and APP_SIDE_FILTER predicates. */
    private static Bbox envelopeOf(Geometry geometry) {
        org.locationtech.jts.geom.Envelope env = geometry.getEnvelopeInternal();
        return Bbox.of2d(env.getMinX(), env.getMinY(), env.getMaxX(), env.getMaxY());
    }

    // --- WKB encoders ---

    /**
     * Little-endian WKB Polygon: one ring of five closed points tracing the axis-aligned rectangle. Ring visits corners
     * counter-clockwise: SW -> SE -> NE -> NW -> SW.
     */
    private static byte[] wkbRect(double minX, double minY, double maxX, double maxY) {
        // 1 (byte-order) + 4 (type) + 4 (numRings) + 4 (numPoints) + 5*16 (coordinates)
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 4 + 4 + 5 * 16).order(ByteOrder.LITTLE_ENDIAN);
        buf.put((byte) 0x01);
        buf.putInt(3);
        buf.putInt(1);
        buf.putInt(5);
        double[][] corners = {{minX, minY}, {maxX, minY}, {maxX, maxY}, {minX, maxY}, {minX, minY}};
        for (double[] xy : corners) {
            buf.putDouble(xy[0]);
            buf.putDouble(xy[1]);
        }
        return buf.array();
    }

    /**
     * Little-endian WKB Polygon: one ring of {@code n} vertices approximating a circle of the given radius centred at
     * (cx, cy), closed by repeating the first vertex.
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

    // --- row holder ---

    /** One row's id and WKB geometry. */
    private record GeometryRow(int id, MemorySegment geom) {}
}
