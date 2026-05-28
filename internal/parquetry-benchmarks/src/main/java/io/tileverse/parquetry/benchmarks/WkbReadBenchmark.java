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

import java.lang.foreign.MemorySegment;
import java.util.concurrent.TimeUnit;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import io.tileverse.parquetry.geo.jts.MemorySegmentWkbReader;

/**
 * Head-to-head decode cost of two WKB-to-JTS readers over one representative geometry per (shape, size) combination.
 *
 * <p>The two readers are compared on identical little-endian WKB bytes:
 *
 * <ul>
 *   <li>{@code JTS_PACKED}: JTS's own {@link WKBReader} wrapping a {@code GeometryFactory} backed by a
 *       {@link PackedCoordinateSequenceFactory}, reading from a pre-built {@code byte[]} - JTS's fastest input path.
 *       The in-tree code feeds {@code WKBReader} from a {@code MemorySegment} adapter instead, which is slower, hence
 *       this is a conservative, favorable-to-JTS baseline.
 *   <li>{@code CUSTOM}: {@link MemorySegmentWkbReader}, which reads directly from a {@link MemorySegment} onto packed
 *       coordinate sequences with one bulk copy per ring, allocating no per-coordinate objects and no intermediate
 *       {@code byte[]}.
 * </ul>
 *
 * <p>Decode time is the headline: the custom reader is roughly an order of magnitude faster on dense geometries. Per-op
 * allocation is near-parity against this packed baseline, since both build the same packed {@code double[]} ring
 * backing. Request {@code -prof gc} to read {@code gc.alloc.rate.norm} (bytes allocated per decode).
 *
 * <pre>{@code
 * ./mvnw -Pbenchmarks -pl :parquetry-benchmarks -am package
 * java --enable-preview --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
 *   -jar internal/parquetry-benchmarks/target/benchmarks.jar WkbReadBenchmark -prof gc
 * }</pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(1)
public class WkbReadBenchmark {

    /** Vertex count for the LARGE arm; mirrors the dense ring of the spatial benchmarks. */
    private static final int LARGE_VERTICES = 2_000;
    /** Vertex count for the LARGE arm under smoke; same code path, far less data. */
    private static final int SMOKE_LARGE_VERTICES = 64;
    /** Vertex count for the SMALL arm: a handful of points, the common case. */
    private static final int SMALL_VERTICES = 12;
    /** Component count for the MULTIPOLYGON shape; each component is one ring of the size's vertex count. */
    private static final int MULTIPOLYGON_PARTS = 8;

    /** The geometry kind to decode. */
    public enum Shape {
        POINT,
        LINESTRING,
        POLYGON,
        MULTIPOLYGON
    }

    /** Per-geometry vertex count: SMALL is a handful of points; LARGE is a dense ring. */
    public enum GeometrySize {
        SMALL,
        LARGE
    }

    /** Which reader decodes the WKB. */
    public enum Reader {
        JTS_PACKED,
        CUSTOM
    }

    @Param({"POINT", "LINESTRING", "POLYGON", "MULTIPOLYGON"})
    private Shape shape;

    @Param({"SMALL", "LARGE"})
    private GeometrySize geometrySize;

    @Param({"JTS_PACKED", "CUSTOM"})
    private Reader reader;

    /** Shrinks the LARGE vertex count for a fast end-to-end check. The default keeps the full measurement workload. */
    @Param({"false"})
    private boolean smoke;

    /** One representative WKB for the (shape, size) combination, wrapped read-only for the custom reader. */
    private MemorySegment wkbSegment;

    /** The same WKB bytes for JTS's WKBReader, which consumes a byte[]. */
    private byte[] wkbBytes;

    /** Reused per trial; WKBReader is not thread-safe but each JMH thread gets its own benchmark state here. */
    private WKBReader jtsReader;

    /** Stateless and thread-safe; one instance suffices. */
    private MemorySegmentWkbReader customReader;

    @Setup(Level.Trial)
    public void setUp() {
        int largeVertices = smoke ? SMOKE_LARGE_VERTICES : LARGE_VERTICES;
        int vertices = (geometrySize == GeometrySize.LARGE) ? largeVertices : SMALL_VERTICES;

        GeometryFactory factory = new GeometryFactory(new PackedCoordinateSequenceFactory());
        Geometry geometry = buildGeometry(factory, shape, vertices);

        WKBWriter writer = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);
        wkbBytes = writer.write(geometry);
        wkbSegment = MemorySegment.ofArray(wkbBytes).asReadOnly();

        jtsReader = new WKBReader(factory);
        customReader = new MemorySegmentWkbReader(factory);
    }

    @Benchmark
    public void decode(Blackhole bh) throws Exception {
        Geometry decoded =
                switch (reader) {
                    case JTS_PACKED -> jtsReader.read(wkbBytes);
                    case CUSTOM -> customReader.read(wkbSegment);
                };
        bh.consume(decoded);
    }

    // --- geometry builders ---

    private static Geometry buildGeometry(GeometryFactory factory, Shape shape, int vertices) {
        return switch (shape) {
            case POINT -> factory.createPoint(new Coordinate(1.0, 2.0));
            case LINESTRING -> buildLineString(factory, vertices);
            case POLYGON -> factory.createPolygon(buildRing(factory, 0.0, 0.0, vertices));
            case MULTIPOLYGON -> buildMultiPolygon(factory, vertices);
        };
    }

    private static LineString buildLineString(GeometryFactory factory, int vertices) {
        Coordinate[] coordinates = new Coordinate[vertices];
        for (int i = 0; i < vertices; i++) {
            coordinates[i] = new Coordinate(i, Math.sin(i));
        }
        return factory.createLineString(coordinates);
    }

    private static Geometry buildMultiPolygon(GeometryFactory factory, int verticesPerRing) {
        Polygon[] polygons = new Polygon[MULTIPOLYGON_PARTS];
        for (int part = 0; part < MULTIPOLYGON_PARTS; part++) {
            double centerX = part * 10.0;
            polygons[part] = factory.createPolygon(buildRing(factory, centerX, 0.0, verticesPerRing));
        }
        return factory.createMultiPolygon(polygons);
    }

    /** A closed ring of {@code vertices} points approximating a unit circle centred at (cx, cy). */
    private static LinearRing buildRing(GeometryFactory factory, double cx, double cy, int vertices) {
        int unique = Math.max(vertices, 4);
        Coordinate[] coordinates = new Coordinate[unique + 1];
        for (int i = 0; i < unique; i++) {
            double angle = 2.0 * Math.PI * i / unique;
            coordinates[i] = new Coordinate(cx + Math.cos(angle), cy + Math.sin(angle));
        }
        coordinates[unique] = coordinates[0];
        return factory.createLinearRing(coordinates);
    }
}
