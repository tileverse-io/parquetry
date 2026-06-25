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

import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
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

import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.GeoParquetMetadataMode;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.GeometryFilter;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Isolates the cost of the exact geometry gate with a synthetic filter backed by no external geometry engine.
 *
 * <p>All rows are written inside a single extent. The query bbox covers that full extent. The spatial pruning tier is
 * therefore a no-op: every row group and every row passes the coarse bbox check. The gate alone decides which rows
 * survive.
 *
 * <p>Two arms:
 *
 * <ul>
 *   <li>{@code gate=off}: reads with a plain {@code bboxIntersects} predicate over the full extent. Every
 *       bbox-candidate row materializes all its output columns ({@code id} plus {@code width} extra INT32 columns).
 *   <li>{@code gate=on}: reads with a {@code geometryFilter} wrapping the synthetic gate. The pruning predicate is the
 *       same full-extent {@code bboxIntersects}, again passing everything. The gate's {@code matches} function accepts
 *       rows deterministically at {@code passRate}: for HIGH roughly 90 % pass, for LOW roughly 10 % pass. Rows the
 *       gate rejects are dropped before their {@code width} output columns are decoded, which is the avoided work this
 *       arm measures.
 * </ul>
 *
 * <p>The {@code width} axis (4 or 16 extra INT32 columns) scales the per-row decode cost of materialization. A wider
 * table makes the gate's savings per rejected row larger. The {@code geometrySize} axis (SMALL 5-point rectangle, LARGE
 * ~2000-vertex ring) scales the cost of the gate's {@code decode} step. Because the benchmark uses a synthetic filter
 * that reads only the first ring vertex's X coordinate from the WKB, LARGE adds a modest decode overhead compared to
 * SMALL.
 *
 * <p>Expected pattern: for {@code gate=on + LOW passRate + wide table}, the gate drops 90 % of rows before their wide
 * output columns are decoded, yielding a large speedup over {@code gate=off}. For {@code gate=on + HIGH passRate + wide
 * table}, nearly every row passes and the gate adds overhead without much benefit.
 *
 * <pre>{@code
 * ./mvnw -Pbenchmarks -pl :parquetry-benchmarks -am package
 * java --enable-preview --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
 *   -jar internal/parquetry-benchmarks/target/benchmarks.jar SpatialGateBenchmark
 * }</pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class SpatialGateBenchmark {

    // --- fixture constants ---

    private static final String GEOMETRY_NAME = "geometry";
    private static final ColumnPath GEOMETRY_COL = ColumnPath.of(GEOMETRY_NAME);
    private static final ColumnPath ID_COL = ColumnPath.of("id");

    /**
     * The full extent that contains all rows. All row geometry centroids are placed inside [0,10]x[0,10]. The query
     * bbox matches this full extent, making every row a bbox candidate and ensuring the pruning tier does not eliminate
     * anything.
     */
    private static final Bbox FULL_EXTENT = Bbox.of2d(0.0, 0.0, 10.0, 10.0);

    private static final int FULL_ROWS_PER_GROUP = 2_000;
    private static final int SMOKE_ROWS_PER_GROUP = 50;
    private static final int NUM_ROW_GROUPS = 8;

    /** Full vertex count for the LARGE polygon ring. */
    private static final int LARGE_RING_VERTICES = 2_000;
    /** Vertex count for the LARGE ring in smoke mode. */
    private static final int SMOKE_RING_VERTICES = 64;

    // --- WKB offset constants: byte-order(1) + type(4) + numRings(4) + numPoints(4) = 13 bytes before first x ---
    private static final int WKB_FIRST_X_OFFSET = 13;

    // --- benchmark parameters ---

    /** Whether the exact geometry gate is active. {@code off} uses a plain bbox predicate; {@code on} adds the gate. */
    @Param({"off", "on"})
    private String gate;

    /** Fraction of rows the gate accepts. HIGH = ~90 %, LOW = ~10 %. */
    @Param({"HIGH", "LOW"})
    private PassRate passRate;

    /** Number of extra INT32 output columns beyond {@code id}. Scales the decode cost of materialization. */
    @Param({"4", "16"})
    private int width;

    /** Per-row geometry complexity. SMALL is a 5-point rectangle; LARGE is a dense ring. */
    @Param({"SMALL", "LARGE"})
    private GeometrySize geometrySize;

    /** Shrinks the fixture for a fast end-to-end correctness check. The default keeps the full measurement workload. */
    @Param({"false"})
    private boolean smoke;

    // --- state ---

    private Path workDir;
    private SyntheticParquet.OpenDataset open;
    private Predicate offPredicate;
    private Predicate onPredicate;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
        int rowsPerGroup = smoke ? SMOKE_ROWS_PER_GROUP : FULL_ROWS_PER_GROUP;
        int ringVertices = smoke ? SMOKE_RING_VERTICES : LARGE_RING_VERTICES;

        workDir = Files.createTempDirectory("parquetry-bench-spatial-gate-");
        Path file = workDir.resolve("spatial-gate.parquet");
        writeFixture(file, rowsPerGroup, ringVertices);
        open = SyntheticParquet.open(file);

        // Both predicates use the full extent; the pruning tier retains every row.
        offPredicate = new Predicate.Spatial.BboxIntersects(GEOMETRY_COL, FULL_EXTENT);
        SyntheticGateFilter gateFilter = new SyntheticGateFilter(passRate.percentAccepted());
        onPredicate = Predicate.geometryFilter(gateFilter);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        open.close();
        SyntheticParquet.deleteRecursively(workDir);
    }

    /**
     * Reads all rows and consumes each surviving row's {@code id} and first output column through the blackhole.
     * Selects the gate-off or gate-on predicate based on the {@code gate} parameter.
     */
    @Benchmark
    public void read(Blackhole bh) {
        Predicate predicate = "on".equals(gate) ? onPredicate : offPredicate;
        try (Stream<ParquetRecord> rows = open.reader().read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            rows.forEach(row -> {
                bh.consume(row.getInt(ID_COL));
                bh.consume(row.getInt(SyntheticParquet.valueColumn(0)));
            });
        }
    }

    // --- fixture writing ---

    private void writeFixture(Path file, int rowsPerGroup, int ringVertices) throws IOException {
        ParquetSchema schema = buildSchema(width);
        WriteOptions options = buildWriteOptions(rowsPerGroup);

        List<GeoRow> rows = buildRows(rowsPerGroup, ringVertices);

        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            for (GeoRow row : rows) {
                appender.setBinary(0, row.geom());
                appender.setInt(1, row.id());
                for (int col = 0; col < row.width(); col++) {
                    appender.setInt(2 + col, (row.id() + col) % 97);
                }
                appender.endRow();
            }
            appender.flush();
        }
    }

    private WriteOptions buildWriteOptions(int rowsPerGroup) {
        WriteOptions.Builder builder = WriteOptions.builder()
                .tempDir(workDir)
                .geoParquetMetadata(GeoParquetMetadataMode.V2_0_ONLY)
                .crsEpsg(GEOMETRY_NAME, 4326)
                .rowGroupSize(RowGroupSize.rows(rowsPerGroup))
                .encodingPolicy(GEOMETRY_NAME, WriteOptions.EncodingPolicy.FORCE_PLAIN)
                .encodingPolicy("id", WriteOptions.EncodingPolicy.FORCE_PLAIN);
        return builder.build();
    }

    /**
     * Builds all rows distributed evenly across {@code NUM_ROW_GROUPS} row groups. All geometries are placed inside the
     * {@code FULL_EXTENT}: no row group has a disjoint bbox from the query.
     */
    private List<GeoRow> buildRows(int rowsPerGroup, int ringVertices) {
        int totalRows = NUM_ROW_GROUPS * rowsPerGroup;
        List<GeoRow> rows = new ArrayList<>(totalRows);
        int id = 0;
        for (int group = 0; group < NUM_ROW_GROUPS; group++) {
            // Spread centroids uniformly inside the extent; different groups use different y-slices.
            double baseY = group * (10.0 / NUM_ROW_GROUPS);
            for (int i = 0; i < rowsPerGroup; i++) {
                double step = 1.0 / rowsPerGroup;
                double cx = (i * step) * 10.0;
                double cy = baseY + step;
                byte[] wkb = buildWkb(cx, cy, ringVertices);
                MemorySegment geom = MemorySegment.ofArray(wkb).asReadOnly();
                rows.add(new GeoRow(id, geom, width));
                id++;
            }
        }
        return rows;
    }

    private byte[] buildWkb(double cx, double cy, int ringVertices) {
        if (geometrySize == GeometrySize.SMALL) {
            double half = 0.001;
            return wkbRect(cx - half, cy - half, cx + half, cy + half);
        }
        return wkbDenseRing(cx, cy, 0.001, ringVertices);
    }

    // --- WKB encoders (private; not shared to avoid coupling to SpatialPruningBenchmark) ---

    /**
     * Little-endian WKB Polygon: one ring of five closed points tracing an axis-aligned rectangle. Ring order: SW -> SE
     * -> NE -> NW -> SW.
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
     * (cx, cy), closed by repeating the first vertex.
     */
    private static byte[] wkbDenseRing(double cx, double cy, double radius, int n) {
        int points = n + 1;
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
        buf.putDouble(cx + radius);
        buf.putDouble(cy);
        return buf.array();
    }

    // --- schema ---

    private static ParquetSchema buildSchema(int valueColumns) {
        List<SchemaNode> leaves = new ArrayList<>(2 + valueColumns);
        leaves.add(primitiveLeaf(GEOMETRY_NAME, PrimitiveKind.BYTE_ARRAY));
        leaves.add(primitiveLeaf("id", PrimitiveKind.INT32));
        for (int i = 0; i < valueColumns; i++) {
            leaves.add(primitiveLeaf("v" + i, PrimitiveKind.INT32));
        }
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, leaves, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive primitiveLeaf(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.REQUIRED, kind, OptionalInt.empty(), Optional.empty(), -1);
    }

    // --- enums ---

    /**
     * Per-row geometry complexity. SMALL is a 5-point axis-aligned rectangle (minimal WKB). LARGE is a dense polygon
     * ring controlled by {@code LARGE_RING_VERTICES} / {@code SMOKE_RING_VERTICES}.
     */
    public enum GeometrySize {
        SMALL,
        LARGE
    }

    /** Acceptance rate for the synthetic gate. HIGH accepts roughly 90 % of rows; LOW accepts roughly 10 %. */
    public enum PassRate {
        HIGH(90),
        LOW(10);

        private final int percentAccepted;

        PassRate(int percentAccepted) {
            this.percentAccepted = percentAccepted;
        }

        int percentAccepted() {
            return percentAccepted;
        }
    }

    // --- synthetic GeometryFilter ---

    /**
     * A synthetic {@link GeometryFilter} that requires no geometry engine. It reads the first ring vertex's X
     * coordinate from the WKB and uses a deterministic hash of that coordinate to accept or reject rows at the target
     * rate.
     *
     * <p>The {@code pruningPredicate} is the full-extent {@code bboxIntersects}, which matches every row, making
     * pruning a no-op. The gate alone controls which rows materialise their output columns.
     */
    private static final class SyntheticGateFilter implements GeometryFilter<Double> {

        private final int percentAccepted;
        private final Predicate.Spatial pruning;

        SyntheticGateFilter(int percentAccepted) {
            this.percentAccepted = percentAccepted;
            this.pruning = new Predicate.Spatial.BboxIntersects(GEOMETRY_COL, FULL_EXTENT);
        }

        @Override
        public ColumnPath column() {
            return GEOMETRY_COL;
        }

        @Override
        public Optional<Predicate.Spatial> pruningPredicate() {
            return Optional.of(pruning);
        }

        /**
         * Reads the first ring vertex's X coordinate from the WKB. The WKB layout for a little-endian Polygon is: byte
         * 0 = byte-order marker, bytes 1-4 = geometry type (INT32), bytes 5-8 = numRings (INT32), bytes 9-12 =
         * numPoints in first ring (INT32), bytes 13-20 = first X (DOUBLE).
         */
        @Override
        public Double decode(MemorySegment wkb) {
            return wkb.get(DOUBLE, WKB_FIRST_X_OFFSET);
        }

        /**
         * Accepts the row when a deterministic hash of the first vertex's X coordinate falls within the target
         * percentile. Using {@code Double.hashCode} over a coordinate yields a well-distributed bucketing without any
         * external dependency.
         */
        @Override
        public boolean matches(Double x) {
            return (Double.hashCode(x) & 0x7fffffff) % 100 < percentAccepted;
        }
    }

    // --- row holder ---

    /** One row's id, WKB geometry, and the count of extra INT32 output columns the writer materializes. */
    private record GeoRow(int id, MemorySegment geom, int width) {}
}
