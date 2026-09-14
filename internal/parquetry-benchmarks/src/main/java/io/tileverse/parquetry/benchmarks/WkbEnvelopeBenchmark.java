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
package io.tileverse.parquetry.benchmarks;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.WKBWriter;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import io.tileverse.parquetry.columnar.BinaryView;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.geo.MemorySegmentWkbReader;
import io.tileverse.parquetry.internal.filter.spatial.WkbEnvelope;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Per-geometry cost of the WKB envelope paths a read pays on every row, over a batch-shaped fixture: one backing
 * segment holding many building-like WKB values, addressed per row by offset and length, which is how a decoded page
 * reaches the decimation gate and the bbox predicate.
 *
 * <ul>
 *   <li>{@link #compute}: the decimation gate's call, the full 2D envelope of each row's WKB read in place;
 *   <li>{@link #computeThroughBatchedCaller}: the same envelope at the call depth of the gate, which is the guard on
 *       the inlining budget described by the WKB cursor's class javadoc;
 *   <li>{@link #matchesDecidedAtFirstVertex}: a whole-world {@code BboxIntersects}, the bbox predicate's production
 *       shape, which the walk decides at the first vertex and therefore isolates the fixed per-call cost;
 *   <li>{@link #matchesFullWalk}: a {@code BboxCoveredBy} with every geometry inside the box, the relation that must
 *       walk every vertex;
 *   <li>{@link #read}: {@link MemorySegmentWkbReader}, the materializer's path, which must not regress.
 * </ul>
 *
 * <p>The defaults pin the buildings point (12 distinct vertices, one polygon, little-endian, native memory); the other
 * axis values run on request, e.g. {@code -p vertices=5,12,40,200 -p byteOrder=LE,BE -p memory=NATIVE,HEAP}. The
 * reported time is per geometry.
 *
 * <pre>{@code
 * ./mvnw -Pbenchmarks -pl :parquetry-benchmarks -am package -DskipTests -ntp
 * java --enable-preview --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
 *   -jar internal/parquetry-benchmarks/target/benchmarks.jar WkbEnvelopeBenchmark
 * }</pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
@OperationsPerInvocation(WkbEnvelopeBenchmark.BATCH_ROWS)
public class WkbEnvelopeBenchmark {

    /** Geometries walked per invocation; the reported time is per geometry. */
    static final int BATCH_ROWS = 1024;
    /** Distinct geometries under smoke; an invocation still walks {@link #BATCH_ROWS} rows by cycling over them. */
    private static final int SMOKE_ROWS = 64;

    private static final int MULTIPOLYGON_PARTS = 3;
    /** A building-sized ring radius in degrees, roughly 50 m at the equator. */
    private static final double RING_RADIUS_DEGREES = 0.0005;

    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");
    private static final Bbox WORLD = Bbox.of2d(-180, -90, 180, 90);
    private static final Predicate.Spatial WORLD_INTERSECTS = new Predicate.Spatial.BboxIntersects(GEOMETRY, WORLD);
    private static final Predicate.Spatial WORLD_COVERED_BY = new Predicate.Spatial.BboxCoveredBy(GEOMETRY, WORLD);

    /** The view constant held by the decimation gate; its arm measures the call made by the gate itself. */
    private static final BinaryView<Bbox> ENVELOPE = WkbEnvelope::compute;

    /** The geometry kind of every row. */
    public enum Shape {
        POLYGON,
        MULTIPOLYGON
    }

    /** The WKB byte order every row is written in. */
    public enum WkbByteOrder {
        LE,
        BE
    }

    /** Where the backing segment lives: a shared native arena (production pages) or a heap array. */
    public enum Memory {
        NATIVE,
        HEAP
    }

    /** Distinct vertices per ring; the closing vertex is added on top. A value below 3 is raised to 3. */
    @Param({"12"})
    private int vertices;

    @Param({"POLYGON"})
    private Shape shape;

    @Param({"LE"})
    private WkbByteOrder byteOrder;

    @Param({"NATIVE"})
    private Memory memory;

    /** Shrinks the fixture to a few distinct rows for a fast end-to-end check; the timing is then meaningless. */
    @Param({"false"})
    private boolean smoke;

    private Arena arena;
    private MemorySegment backing;
    private long[] offsets;
    private long[] lengths;
    private int rowMask;
    private MemorySegmentWkbReader reader;

    @Setup(Level.Trial)
    public void setUp() {
        int rows = smoke ? SMOKE_ROWS : BATCH_ROWS;
        if (Integer.bitCount(rows) != 1) {
            throw new IllegalStateException("Row count must be a power of two for the row mask: " + rows);
        }
        rowMask = rows - 1;
        byte[][] values = encodeBatch(rows);
        offsets = new long[rows];
        lengths = new long[rows];
        long total = 0;
        for (int i = 0; i < rows; i++) {
            offsets[i] = total;
            lengths[i] = values[i].length;
            total += values[i].length;
        }
        byte[] concatenated = new byte[Math.toIntExact(total)];
        for (int i = 0; i < rows; i++) {
            System.arraycopy(values[i], 0, concatenated, Math.toIntExact(offsets[i]), values[i].length);
        }
        backing = allocateBacking(concatenated);
        reader = new MemorySegmentWkbReader();
    }

    private MemorySegment allocateBacking(byte[] concatenated) {
        MemorySegment heap = MemorySegment.ofArray(concatenated);
        if (memory == Memory.HEAP) {
            return heap.asReadOnly();
        }
        arena = Arena.ofShared();
        MemorySegment nativeSegment = arena.allocate(concatenated.length);
        MemorySegment.copy(heap, 0L, nativeSegment, 0L, concatenated.length);
        return nativeSegment.asReadOnly();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (arena != null) {
            arena.close();
        }
    }

    @Benchmark
    public void compute(Blackhole bh) {
        for (int i = 0; i < BATCH_ROWS; i++) {
            int row = i & rowMask;
            bh.consume(WkbEnvelope.compute(backing, offsets[row], lengths[row]));
        }
    }

    /**
     * The gate's envelope call at the gate's own call depth: a decoded batch reaches the envelope through a vector's
     * read method and the {@link BinaryView} handed to it, two frames below the loop over the rows. A score near
     * {@link #compute} means that path is on the fast path; a score several times higher means a frame was added and
     * the coordinate read no longer compiles to a plain load.
     */
    @Benchmark
    public void computeThroughBatchedCaller(Blackhole bh) {
        for (int i = 0; i < BATCH_ROWS; i++) {
            int row = i & rowMask;
            bh.consume(readValue(ENVELOPE, backing, offsets[row], lengths[row]));
        }
    }

    /** Stands in for a binary vector's read of one row: the frame between the row loop and the view. */
    private static <R> R readValue(BinaryView<R> view, MemorySegment backing, long offset, long length) {
        return view.read(backing, offset, length);
    }

    @Benchmark
    public void matchesDecidedAtFirstVertex(Blackhole bh) {
        for (int i = 0; i < BATCH_ROWS; i++) {
            int row = i & rowMask;
            bh.consume(WkbEnvelope.matches(WORLD_INTERSECTS, backing, offsets[row], lengths[row]));
        }
    }

    @Benchmark
    public void matchesFullWalk(Blackhole bh) {
        for (int i = 0; i < BATCH_ROWS; i++) {
            int row = i & rowMask;
            bh.consume(WkbEnvelope.matches(WORLD_COVERED_BY, backing, offsets[row], lengths[row]));
        }
    }

    @Benchmark
    public void read(Blackhole bh) {
        for (int i = 0; i < BATCH_ROWS; i++) {
            int row = i & rowMask;
            bh.consume(reader.read(backing, offsets[row], lengths[row]));
        }
    }

    // --- fixture ---

    private byte[][] encodeBatch(int rows) {
        GeometryFactory factory = new GeometryFactory(new PackedCoordinateSequenceFactory());
        int jtsByteOrder = byteOrder == WkbByteOrder.LE ? ByteOrderValues.LITTLE_ENDIAN : ByteOrderValues.BIG_ENDIAN;
        WKBWriter writer = new WKBWriter(2, jtsByteOrder);
        SplittableRandom random = new SplittableRandom(42);
        byte[][] values = new byte[rows][];
        for (int i = 0; i < rows; i++) {
            values[i] = writer.write(buildGeometry(factory, random));
        }
        return values;
    }

    /** One building somewhere on the globe: a jittered ring, or three of them side by side. */
    private Geometry buildGeometry(GeometryFactory factory, SplittableRandom random) {
        double centerX = random.nextDouble(-180.0, 180.0);
        double centerY = random.nextDouble(-85.0, 85.0);
        if (shape == Shape.POLYGON) {
            return factory.createPolygon(buildRing(factory, random, centerX, centerY));
        }
        Polygon[] parts = new Polygon[MULTIPOLYGON_PARTS];
        for (int part = 0; part < MULTIPOLYGON_PARTS; part++) {
            double partX = centerX + part * 3.0 * RING_RADIUS_DEGREES;
            parts[part] = factory.createPolygon(buildRing(factory, random, partX, centerY));
        }
        return factory.createMultiPolygon(parts);
    }

    /** A closed ring of {@code vertices} distinct points on a jittered circle: the outline of a building. */
    private LinearRing buildRing(GeometryFactory factory, SplittableRandom random, double centerX, double centerY) {
        int distinct = Math.max(vertices, 3);
        Coordinate[] coordinates = new Coordinate[distinct + 1];
        for (int i = 0; i < distinct; i++) {
            double angle = 2.0 * Math.PI * i / distinct;
            double radius = RING_RADIUS_DEGREES * random.nextDouble(0.7, 1.3);
            coordinates[i] = new Coordinate(centerX + radius * Math.cos(angle), centerY + radius * Math.sin(angle));
        }
        coordinates[distinct] = coordinates[0];
        return factory.createLinearRing(coordinates);
    }
}
