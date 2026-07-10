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
package io.tileverse.parquetry.internal.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
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

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.GeoParquetMetadataMode;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.GeometryFilter;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.explain.Tier;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.Wkb;

/**
 * A parquetry-written GeoParquet file has native per-row-group geometry bounds (no covering columns). A surviving
 * {@link Predicate.Spatial} leaf must therefore eliminate the row groups whose union bounding box proves no row can
 * match, at {@link Tier#SPATIAL}.
 *
 * <p>The fixture writes four row groups, each clustered in a distinct square region: rg0 around [0..10], rg1 around
 * [100..110], rg2 around [200..210], rg3 around [300..310].
 */
class SpatialRowGroupPruningTest {

    private static final int ROWS_PER_GROUP = 4;

    @TempDir
    Path tempDir;

    @Test
    void intersectsEliminatesNonOverlappingRowGroups() throws Exception {
        Path file = writeFourClusteredRowGroups();

        ReadOptions options = ReadOptions.DEFAULTS;
        Predicate predicate = Pred.col("geometry").bboxIntersects(Bbox.of2d(0, 0, 10, 10));

        List<PruningDecision> decisions = planDecisions(file, predicate, options);
        List<Integer> ids = readIds(file, predicate, options);

        assertThat(spatialEliminations(decisions))
                .as("rg1, rg2 and rg3 are spatially disjoint from the query box and must be eliminated")
                .isEqualTo(3);
        assertThat(ids)
                .as("only the rows clustered in [0..10]x[0..10] (row group 0) match")
                .containsExactlyInAnyOrder(0, 1, 2, 3);
    }

    @Test
    void containsEliminatesRowGroupsWhoseUnionCannotEncloseTheQuery() throws Exception {
        Path file = writeFourClusteredRowGroups();

        ReadOptions options = ReadOptions.DEFAULTS;
        // A query box that only row group 2's geometries can possibly enclose.
        Predicate predicate = Pred.col("geometry").bboxContains(Bbox.of2d(204, 204, 204.5, 204.5));

        List<PruningDecision> decisions = planDecisions(file, predicate, options);
        List<Integer> ids = readIds(file, predicate, options);

        assertThat(spatialEliminations(decisions))
                .as("the three row groups whose union bbox cannot enclose the query must be eliminated")
                .isEqualTo(3);
        assertThat(ids)
                .as("only rows whose own rectangle encloses [204,204,204.5,204.5] match")
                .isNotEmpty()
                .allMatch(id -> id >= ROWS_PER_GROUP * 2 && id < ROWS_PER_GROUP * 3);
    }

    @Test
    void geometryFilterPruningPredicateEliminatesDisjointRowGroupsAtSpatialTier() throws Exception {
        Path file = writeFourClusteredRowGroups();

        ColumnPath geomCol = ColumnPath.of("geometry");
        // A query box that is disjoint from rg1, rg2, and rg3 - only rg0 overlaps [0..10].
        Bbox disjointFromMostGroups = Bbox.of2d(0, 0, 10, 10);
        Predicate.Spatial pruning = new Predicate.Spatial.BboxIntersects(geomCol, disjointFromMostGroups);
        GeometryFilter<Object> fake = fakePruningOnlyFilter(geomCol, Optional.of(pruning));
        Predicate predicate = Predicate.geometryFilter(fake);

        ReadOptions options = ReadOptions.DEFAULTS;

        List<PruningDecision> decisions = planDecisions(file, predicate, options);

        assertThat(spatialEliminations(decisions))
                .as(
                        "the GeometryFilterPredicate's pruning predicate must drive SPATIAL-tier elimination of disjoint row groups")
                .isEqualTo(3);
    }

    private static long spatialEliminations(List<PruningDecision> decisions) {
        return decisions.stream()
                .filter(d -> d.tier() == Tier.SPATIAL)
                .filter(d -> d instanceof PruningDecision.Eliminated)
                .count();
    }

    /**
     * Returns a fake {@link GeometryFilter} whose {@code pruningPredicate()} returns {@code lowering}. The gate keeps
     * every row that reaches it (decode returns a sentinel, matches always accepts it). This isolates the pruning
     * assertion: only the spatial-tier elimination count matters here, not the exact row set.
     */
    private static GeometryFilter<Object> fakePruningOnlyFilter(
            ColumnPath geomCol, Optional<Predicate.Spatial> lowering) {
        return new GeometryFilter<>() {
            public ColumnPath column() {
                return geomCol;
            }

            public Optional<Predicate.Spatial> pruningPredicate() {
                return lowering;
            }

            public Object decode(MemorySegment wkb) {
                return Boolean.TRUE;
            }

            public boolean matches(Object geometry) {
                return true;
            }
        };
    }

    private Path writeFourClusteredRowGroups() throws Exception {
        ParquetSchema schema = flatSchema(requiredBinary("geometry"), requiredInt32("id"));
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .geoParquetMetadata(GeoParquetMetadataMode.V2_0_ONLY)
                .crsEpsg("geometry", 4326)
                .rowGroupSize(RowGroupSize.rows(ROWS_PER_GROUP))
                .build();
        Path file = tempDir.resolve("clustered.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            writeCluster(writer, schema, 0, 0.0);
            writeCluster(writer, schema, 1, 100.0);
            writeCluster(writer, schema, 2, 200.0);
            writeCluster(writer, schema, 3, 300.0);
        }
        return file;
    }

    /**
     * Writes {@link #ROWS_PER_GROUP} unit rectangles spread across the cluster's [origin..origin+10] square as one
     * batch, which the {@link RowGroupSize#rows(int)} policy turns into a single dedicated row group.
     */
    private static void writeCluster(ParquetFileWriter writer, ParquetSchema schema, int clusterIndex, double origin)
            throws IOException {
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (int i = 0; i < ROWS_PER_GROUP; i++) {
            int id = clusterIndex * ROWS_PER_GROUP + i;
            double minX = origin + i * 2.0;
            double minY = origin + i * 2.0;
            String wkt = rectangleWkt(minX, minY, minX + 1.0, minY + 1.0);
            rows.add(Map.of(ColumnPath.of("geometry"), Wkb.fromWkt(wkt), ColumnPath.of("id"), id));
        }
        writer.writeBatch(WriteFixtures.batch(schema, rows));
    }

    private static List<Integer> readIds(Path file, Predicate predicate, ReadOptions options) {
        List<Integer> ids = new ArrayList<>();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader dataset = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows = dataset.read(predicate, Projection.ALL, options)) {
                rows.forEach(row -> ids.add(row.getInt(ColumnPath.of("id"))));
            }
        }
        return ids;
    }

    private static List<PruningDecision> planDecisions(Path file, Predicate predicate, ReadOptions options) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader dataset = ParquetFileReader.open(source);
            ExplainPlan plan = dataset.explain(predicate, Projection.ALL, options);
            return plan.rowGroups().stream()
                    .flatMap(rowGroup -> rowGroup.tiers().stream())
                    .toList();
        }
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

    /** Closed-ring rectangular polygon in WKT, corners walked counter-clockwise: SW -> SE -> NE -> NW -> SW. */
    private static String rectangleWkt(double minX, double minY, double maxX, double maxY) {
        return "POLYGON ((%s %s, %s %s, %s %s, %s %s, %s %s))"
                .formatted(minX, minY, maxX, minY, maxX, maxY, minX, maxY, minX, minY);
    }
}
