/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.geo;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * End-to-end spatial read over GeoParquet files that carry a {@code bbox} covering struct, for both the GeoParquet 1.1
 * file that declares {@code covering} metadata and the GeoParquet 1.0 file that only exposes the conventional
 * {@code bbox} struct (Overture's on-disk shape). Both must prune row groups via the covering columns and return
 * exactly the rows whose geometry intersects the query - the same rows a brute-force scan finds. This guards two
 * read-path behaviors together: lowering a spatial filter onto the covering columns, and resolving a struct-nested
 * column at record level.
 */
class BboxCoveringSpatialReadTest {

    private static final String COVERING_1_1 = "parquetry/geo/buildings-gp110-bbox-covering.parquet";
    private static final String CONVENTIONAL_1_0 = "parquetry/geo/buildings-gp100-bbox-nocovering.parquet";

    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");
    private static final long TOTAL_ROWS = 1801L;

    @TempDir
    Path tempDir;

    @ParameterizedTest
    @ValueSource(strings = {COVERING_1_1, CONVENTIONAL_1_0})
    void spatialReadPrunesAndReturnsTheSameRowsAsABruteForceScan(String resource) {
        Path file = TestCorpus.extractFile(resource, tempDir);
        JtsGeometryFilter filter = JtsGeometryFilter.intersects(GEOMETRY, queryDiamond());
        Predicate predicate = Predicate.geometryFilter(filter);

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            long expected = bruteForceMatches(reader, filter);
            long pushedDown = matchingRowCount(reader, predicate);
            long scanned = reader.explain(predicate, Projection.ALL, ReadOptions.DEFAULTS)
                    .estimatedRowsScanned();

            assertThat(expected)
                    .as("the query must select a strict, non-empty subset to be a meaningful test")
                    .isGreaterThan(0L)
                    .isLessThan(TOTAL_ROWS);
            assertThat(pushedDown)
                    .as("pushed-down spatial read must return exactly the brute-force matches")
                    .isEqualTo(expected);
            assertThat(scanned)
                    .as("covering pruning must narrow the scan below the full file")
                    .isLessThan(TOTAL_ROWS);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {COVERING_1_1, CONVENTIONAL_1_0})
    void readMatchesCountForAPredicateOnTheNestedBboxColumn(String resource) {
        Path file = TestCorpus.extractFile(resource, tempDir);
        Predicate everyRow =
                io.tileverse.parquetry.filter.Pred.col("bbox", "ymax").gtEq(-100.0);

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            assertThat(matchingRowCount(reader, everyRow))
                    .as("read() on a struct-nested column must agree with count() and see every row")
                    .isEqualTo(reader.count(everyRow, ReadOptions.DEFAULTS))
                    .isEqualTo(TOTAL_ROWS);
        }
    }

    private static long matchingRowCount(ParquetFileReader reader, Predicate predicate) {
        try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.count();
        }
    }

    private static long bruteForceMatches(ParquetFileReader reader, JtsGeometryFilter filter) {
        try (Stream<ParquetRecord> rows = reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.filter(row -> intersects(filter, row)).count();
        }
    }

    private static boolean intersects(JtsGeometryFilter filter, ParquetRecord row) {
        if (row.get(GEOMETRY) instanceof MemorySegment wkb) {
            return filter.gate(wkb).isPresent();
        }
        return false;
    }

    /** A diamond over the dense centre of the San Salvador extract, matching the probe's query shape. */
    private static Geometry queryDiamond() {
        double cx = -89.200;
        double cy = 13.700;
        double r = 0.002;
        GeometryFactory factory = new GeometryFactory();
        Coordinate[] ring = {
            new Coordinate(cx, cy + r),
            new Coordinate(cx + r, cy),
            new Coordinate(cx, cy - r),
            new Coordinate(cx - r, cy),
            new Coordinate(cx, cy + r)
        };
        return factory.createPolygon(ring);
    }
}
