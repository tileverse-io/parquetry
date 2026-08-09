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
import io.tileverse.parquetry.data.WriteOptions.CoveringMode;
import io.tileverse.parquetry.data.WriteOptions.GeoParquetMetadataMode;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Bbox;
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
import io.tileverse.parquetry.schema.geo.geoparquet.BboxCovering;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.testsupport.Wkb;

/**
 * End-to-end proof that a caller who supplies only a geometry column, plus {@link CoveringMode#AUTO}, gets a written
 * {@code bbox} covering that the reader can then use for page pruning. The writer derives the covering; the caller
 * never hands it a bbox column. The fixture spreads points across a single large row group split into several data
 * pages, with only the earliest page overlapping the query window, and asserts that (1) the COLUMN_INDEX tier narrows
 * the surviving rows to a strict subset of pages, (2) the filtered read returns exactly the truly-overlapping rows, and
 * (3) the written {@code geo} metadata declares the covering with FLOAT leaves under a WGS84 geometry.
 */
class BboxCoveringWriteRoundTripIT {

    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");
    private static final ColumnPath ID = ColumnPath.of("id");

    private static final int ROW_COUNT = 500;
    private static final int ROWS_PER_PAGE = 100;
    private static final Bbox QUERY_WINDOW = Bbox.of2d(0, 0, 10, 10);

    @TempDir
    Path tempDir;

    @Test
    void autoCoveringPrunesPagesAndReturnsOverlappingRows() throws Exception {
        Path file = writeMarchingPointsWithAutoCovering();
        Predicate predicate = Pred.col("geometry").bboxIntersects(QUERY_WINDOW);

        List<PruningDecision> decisions = planDecisions(file, predicate);
        assertThat(decisions)
                .as("the covering columns must let the page tier narrow the single row group to the overlapping pages")
                .anyMatch(decision ->
                        decision.tier() == Tier.COLUMN_INDEX && decision instanceof PruningDecision.NarrowedTo);

        assertThat(readIds(file, predicate))
                .as("the derived covering drops no truly overlapping row and admits no other")
                .containsExactlyElementsOf(expectedOverlappingIds());
    }

    @Test
    void autoCoveringDeclaresFloatCoveringInGeoMetadata() throws Exception {
        Path file = writeMarchingPointsWithAutoCovering();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            String geoJson = reader.keyValueMetadata().get("geo");
            assertThat(geoJson).as("V1_1 metadata must be present").isNotNull();
            GeoParquetMetadata parsed = GeoParquetMetadata.parse(geoJson);
            BboxCovering covering =
                    parsed.columns().get("geometry").covering().orElseThrow().bbox();
            assertThat(covering.xmin()).isEqualTo(ColumnPath.of("bbox", "xmin"));
            assertThat(covering.ymin()).isEqualTo(ColumnPath.of("bbox", "ymin"));
            assertThat(covering.xmax()).isEqualTo(ColumnPath.of("bbox", "xmax"));
            assertThat(covering.ymax()).isEqualTo(ColumnPath.of("bbox", "ymax"));

            SchemaNode.Primitive xminLeaf = (SchemaNode.Primitive)
                    reader.schema().find(ColumnPath.of("bbox", "xmin")).orElseThrow();
            assertThat(xminLeaf.kind())
                    .as("AUTO over a WGS84 geometry writes a FLOAT covering")
                    .isEqualTo(PrimitiveKind.FLOAT);
        }
    }

    private Path writeMarchingPointsWithAutoCovering() throws Exception {
        ParquetSchema schema = geometryAndIdSchema();
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .geoParquetMetadata(GeoParquetMetadataMode.V1_1_ONLY)
                .crsEpsg("geometry", 4326)
                .bboxCovering(CoveringMode.AUTO)
                .rowGroupSize(RowGroupSize.rows(10 * ROW_COUNT))
                .pageValueLimit(ROWS_PER_PAGE)
                .build();
        Path file = tempDir.resolve("auto-covering.parquet");
        List<Map<ColumnPath, Object>> rows = marchingPointRows();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.writeBatch(WriteFixtures.batch(schema, rows));
        }
        return file;
    }

    /**
     * One point per row, marching along +X while holding Y at the middle of the query window. Point {@code i} sits at
     * {@code (i, 5)}. Only the first {@value #ROWS_PER_PAGE}-row page holds points inside the query window. Every later
     * page is spatially disjoint from it.
     */
    private static List<Map<ColumnPath, Object>> marchingPointRows() {
        List<Map<ColumnPath, Object>> rows = new ArrayList<>(ROW_COUNT);
        for (int id = 0; id < ROW_COUNT; id++) {
            String wkt = "POINT (%d 5)".formatted(id);
            rows.add(Map.of(GEOMETRY, Wkb.fromWkt(wkt), ID, id));
        }
        return rows;
    }

    /** The point {@code (i, 5)} overlaps {@code [0,0,10,10]} exactly when {@code 0 <= i <= 10}. */
    private static List<Integer> expectedOverlappingIds() {
        List<Integer> ids = new ArrayList<>();
        for (int id = 0; id <= 10; id++) {
            ids.add(id);
        }
        return ids;
    }

    private static List<PruningDecision> planDecisions(Path file, Predicate predicate) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            ExplainPlan plan = reader.explain(predicate, Projection.ALL, ReadOptions.DEFAULTS);
            return plan.rowGroups().stream()
                    .flatMap(rowGroup -> rowGroup.tiers().stream())
                    .toList();
        }
    }

    private static List<Integer> readIds(Path file, Predicate predicate) {
        List<Integer> ids = new ArrayList<>();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
                rows.forEach(row -> ids.add(row.getInt(ID)));
            }
        }
        return ids;
    }

    private static ParquetSchema geometryAndIdSchema() {
        SchemaNode.Group root = new SchemaNode.Group(
                "schema",
                Repetition.REQUIRED,
                List.of(requiredBinary("geometry"), requiredInt32("id")),
                Optional.empty(),
                -1);
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
