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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.GeometryColumns;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Bbox predicates over the {@code apache/parquet-testing} geospatial corpus agree with a planar brute force. The corpus
 * has GEOMETRY and GEOGRAPHY columns with native row-group bounding boxes, some of them wrapping the antimeridian. The
 * brute force decodes each row's WKB with JTS and applies the record-level definition of each relation to the row's
 * vertices. Pruning on the native boxes must keep the rows accepted by the brute force, and {@code bounds} must enclose
 * them.
 */
class GeospatialCorpusConformanceIT {

    private static final String CORPUS = "parquet-testing/data/geospatial";
    private static final double GRID_STEP = 15;
    private static final double[] QUERY_SIZES = {5, 15};
    private static final int MAX_ROW_ENVELOPE_QUERIES = 50;

    @TempDir
    static Path corpusDir;

    @BeforeAll
    static void extractCorpus() {
        TestCorpus.extractDirectory(CORPUS, corpusDir);
    }

    static Stream<String> filesWithAGeometryColumn() throws IOException {
        List<String> names;
        try (Stream<Path> files = Files.list(corpusDir)) {
            names = files.filter(path -> path.toString().endsWith(".parquet"))
                    .filter(GeospatialCorpusConformanceIT::hasGeometryColumn)
                    .map(path -> path.getFileName().toString())
                    .sorted()
                    .toList();
        }
        return names.stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("filesWithAGeometryColumn")
    void bboxRelationsMatchThePlanarBruteForce(String fileName) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(corpusDir.resolve(fileName))) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            ColumnPath geometry = geometryColumnOf(reader.schema());
            List<RowExtents> rows = readRowExtents(reader, geometry);
            boolean geography = isGeography(reader.schema(), geometry);

            List<String> mismatches = new ArrayList<>();
            for (Bbox query : queryBoxes(rows, geography)) {
                for (Relation relation : Relation.values()) {
                    countMismatch(reader, geometry, rows, relation, query).ifPresent(mismatches::add);
                }
            }
            assertThat(mismatches)
                    .as("%s: bbox queries disagreeing with the brute force", fileName)
                    .isEmpty();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("filesWithAGeometryColumn")
    void boundsEncloseTheMatchingRows(String fileName) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(corpusDir.resolve(fileName))) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            ColumnPath geometry = geometryColumnOf(reader.schema());
            List<RowExtents> rows = readRowExtents(reader, geometry);

            Optional<Bbox> allRows = extentOf(rows);
            // Null counts prove IsNotNull for whole row groups, and bounds adds the boxes of those groups unscanned.
            Predicate provenByStatistics = new Predicate.IsNotNull(geometry);

            List<String> violations = new ArrayList<>();
            boundsViolation(reader, Predicate.ALWAYS_TRUE, allRows).ifPresent(violations::add);
            boundsViolation(reader, provenByStatistics, allRows).ifPresent(violations::add);
            for (Bbox query : lonLatGrid()) {
                Predicate predicate = Relation.INTERSECTS.predicate(geometry, query);
                List<RowExtents> matching = Relation.INTERSECTS.matchingRows(rows, query);
                boundsViolation(reader, predicate, extentOf(matching)).ifPresent(violations::add);
            }
            assertThat(violations)
                    .as("%s: bounds not enclosing the matching rows", fileName)
                    .isEmpty();
        }
    }

    private static Optional<String> countMismatch(
            ParquetFileReader reader, ColumnPath geometry, List<RowExtents> rows, Relation relation, Bbox query) {
        Predicate predicate = relation.predicate(geometry, query);
        long read = countReadRows(reader, geometry, predicate);
        long expected = relation.matchingRows(rows, query).size();
        if (read == expected) {
            return Optional.empty();
        }
        return Optional.of("%s %s: read %d rows, brute force %d".formatted(relation, query, read, expected));
    }

    private static long countReadRows(ParquetFileReader reader, ColumnPath geometry, Predicate predicate) {
        Projection geometryOnly = Projection.ofPhysical(List.of(geometry));
        try (Stream<ParquetRecord> records = reader.read(predicate, geometryOnly, ReadOptions.DEFAULTS)) {
            return records.count();
        }
    }

    /** A planar bound must enclose the matching rows; with no matching row any answer is sound. */
    private static Optional<String> boundsViolation(
            ParquetFileReader reader, Predicate predicate, Optional<Bbox> matchingExtent) {
        if (matchingExtent.isEmpty()) {
            return Optional.empty();
        }
        Bbox extent = matchingExtent.orElseThrow();
        Optional<BoundingBox> bounds = reader.bounds(predicate, ReadOptions.DEFAULTS);
        if (bounds.isPresent() && encloses(bounds.orElseThrow(), extent)) {
            return Optional.empty();
        }
        return Optional.of("%s: bounds %s, matching rows span %s".formatted(predicate, bounds, extent));
    }

    private static boolean encloses(BoundingBox bounds, Bbox extent) {
        return bounds.xmin() <= extent.minX()
                && bounds.xmax() >= extent.maxX()
                && bounds.ymin() <= extent.minY()
                && bounds.ymax() >= extent.maxY();
    }

    /**
     * The lon/lat grid, plus the whole world and the extent of the file's rows. A GEOMETRY column adds the exact
     * envelopes of a sample of rows, the queries giving the equality relation a match. A GEOGRAPHY column does not: the
     * corpus computes its row-group boxes on the sphere, and those miss some vertices by a few ulps. A query edge
     * placed exactly on such a vertex would test that rounding rather than the pruning.
     */
    private static List<Bbox> queryBoxes(List<RowExtents> rows, boolean geography) {
        List<Bbox> queries = new ArrayList<>(lonLatGrid());
        queries.add(Bbox.of2d(-180, -90, 180, 90));
        extentOf(rows).ifPresent(queries::add);
        if (!geography) {
            queries.addAll(sampledRowEnvelopes(rows));
        }
        return queries;
    }

    /**
     * Square boxes of each query size on a regular grid. The grid includes boxes ending on and starting from each edge
     * of the lon/lat range, placing boxes on both sides of the antimeridian.
     */
    private static List<Bbox> lonLatGrid() {
        List<Bbox> boxes = new ArrayList<>();
        for (double size : QUERY_SIZES) {
            for (double minX : gridStarts(-180, 180, size)) {
                for (double minY : gridStarts(-90, 90, size)) {
                    boxes.add(Bbox.of2d(minX, minY, minX + size, minY + size));
                }
            }
        }
        return boxes;
    }

    /** {@code low - size} for a box ending on the low edge, then {@code low} to {@code high} by the grid step. */
    private static List<Double> gridStarts(double low, double high, double size) {
        List<Double> starts = new ArrayList<>();
        starts.add(low - size);
        for (double start = low; start <= high; start += GRID_STEP) {
            starts.add(start);
        }
        return starts;
    }

    private static List<Bbox> sampledRowEnvelopes(List<RowExtents> rows) {
        List<Bbox> envelopes = new ArrayList<>();
        int stride = Math.max(1, rows.size() / MAX_ROW_ENVELOPE_QUERIES);
        for (int i = 0; i < rows.size(); i += stride) {
            Bbox envelope = rows.get(i).envelope();
            if (envelope.minX() <= envelope.maxX() && envelope.minY() <= envelope.maxY()) {
                envelopes.add(envelope);
            }
        }
        return envelopes;
    }

    /** The planar extent of the rows' envelopes, or empty when no row has a vertex on both axes. */
    private static Optional<Bbox> extentOf(List<RowExtents> rows) {
        double minX = Double.POSITIVE_INFINITY;
        double minY = Double.POSITIVE_INFINITY;
        double maxX = Double.NEGATIVE_INFINITY;
        double maxY = Double.NEGATIVE_INFINITY;
        for (RowExtents row : rows) {
            Bbox envelope = row.envelope();
            minX = Math.min(minX, envelope.minX());
            minY = Math.min(minY, envelope.minY());
            maxX = Math.max(maxX, envelope.maxX());
            maxY = Math.max(maxY, envelope.maxY());
        }
        if (minX > maxX || minY > maxY) {
            return Optional.empty();
        }
        return Optional.of(Bbox.of2d(minX, minY, maxX, maxY));
    }

    private static boolean hasGeometryColumn(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetSchema schema = ParquetFileReader.open(source).schema();
            return !GeometryColumns.resolve(schema, Optional.empty()).isEmpty();
        }
    }

    private static ColumnPath geometryColumnOf(ParquetSchema schema) {
        Set<ColumnPath> columns = GeometryColumns.resolve(schema, Optional.empty());
        assertThat(columns).as("one GEOMETRY or GEOGRAPHY column").hasSize(1);
        return columns.iterator().next();
    }

    private static boolean isGeography(ParquetSchema schema, ColumnPath column) {
        SchemaNode.Primitive leaf = (SchemaNode.Primitive) schema.find(column).orElseThrow();
        Optional<LogicalType> logicalType = leaf.logicalType();
        return logicalType.isPresent() && logicalType.orElseThrow() instanceof LogicalType.Geography;
    }

    /** The extents of each row holding a geometry, in file order; a null geometry matches no relation. */
    private static List<RowExtents> readRowExtents(ParquetFileReader reader, ColumnPath geometry) {
        List<RowExtents> rows = new ArrayList<>();
        Projection geometryOnly = Projection.ofPhysical(List.of(geometry));
        try (Stream<ParquetRecord> records = reader.read(Predicate.ALWAYS_TRUE, geometryOnly, ReadOptions.DEFAULTS)) {
            records.forEach(row -> wkbOf(row, geometry).map(RowExtents::of).ifPresent(rows::add));
        }
        return rows;
    }

    private static Optional<byte[]> wkbOf(ParquetRecord row, ColumnPath geometry) {
        byte[] wkb = row.readBinary(
                geometry,
                (backing, offset, length) -> backing.asSlice(offset, length).toArray(JAVA_BYTE));
        return Optional.ofNullable(wkb);
    }

    /** The four bbox relations, each with its predicate and its brute-force record-level answer. */
    private enum Relation {
        INTERSECTS {
            @Override
            Predicate predicate(ColumnPath column, Bbox query) {
                return new Predicate.Spatial.BboxIntersects(column, query);
            }

            @Override
            boolean matches(RowExtents row, Bbox query) {
                return row.envelope().intersects(query);
            }
        },
        CONTAINS {
            @Override
            Predicate predicate(ColumnPath column, Bbox query) {
                return new Predicate.Spatial.BboxContains(column, query);
            }

            @Override
            boolean matches(RowExtents row, Bbox query) {
                return row.envelope().contains(query);
            }
        },
        COVERED_BY {
            @Override
            Predicate predicate(ColumnPath column, Bbox query) {
                return new Predicate.Spatial.BboxCoveredBy(column, query);
            }

            @Override
            boolean matches(RowExtents row, Bbox query) {
                return row.hasRealVertex() && row.realVertexEnvelope().coveredBy(query);
            }
        },
        EQUALS {
            @Override
            Predicate predicate(ColumnPath column, Bbox query) {
                return new Predicate.Spatial.BboxEquals(column, query);
            }

            @Override
            boolean matches(RowExtents row, Bbox query) {
                return row.envelope().sameBox2d(query);
            }
        };

        abstract Predicate predicate(ColumnPath column, Bbox query);

        abstract boolean matches(RowExtents row, Bbox query);

        List<RowExtents> matchingRows(List<RowExtents> rows, Bbox query) {
            List<RowExtents> matching = new ArrayList<>();
            for (RowExtents row : rows) {
                if (matches(row, query)) {
                    matching.add(row);
                }
            }
            return matching;
        }
    }

    /**
     * The planar extents of one row's vertices, following the record-level definitions. The envelope widens each axis
     * by its non-NaN ordinates, as the envelope-based relations do. The real-vertex envelope spans only vertices with
     * both ordinates present, as the covered-by relation does; an empty geometry has neither.
     */
    private record RowExtents(Bbox envelope, Bbox realVertexEnvelope, boolean hasRealVertex) {

        static RowExtents of(byte[] wkb) {
            Extent envelope = Extent.EMPTY;
            Extent realVertices = Extent.EMPTY;
            boolean hasRealVertex = false;
            for (Coordinate vertex : decode(wkb).getCoordinates()) {
                envelope = envelope.widenedBy(vertex.x, vertex.y);
                if (!Double.isNaN(vertex.x) && !Double.isNaN(vertex.y)) {
                    realVertices = realVertices.widenedBy(vertex.x, vertex.y);
                    hasRealVertex = true;
                }
            }
            return new RowExtents(envelope.toBbox(), realVertices.toBbox(), hasRealVertex);
        }

        private static Geometry decode(byte[] wkb) {
            try {
                return new WKBReader().read(wkb);
            } catch (ParseException e) {
                throw new AssertionError("corpus WKB rejected by JTS", e);
            }
        }
    }

    /** A running planar extent; a NaN ordinate leaves its axis unchanged. */
    private record Extent(double minX, double minY, double maxX, double maxY) {

        static final Extent EMPTY = new Extent(
                Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY);

        Extent widenedBy(double x, double y) {
            return new Extent(x < minX ? x : minX, y < minY ? y : minY, x > maxX ? x : maxX, y > maxY ? y : maxY);
        }

        Bbox toBbox() {
            return Bbox.of2d(minX, minY, maxX, maxY);
        }
    }
}
