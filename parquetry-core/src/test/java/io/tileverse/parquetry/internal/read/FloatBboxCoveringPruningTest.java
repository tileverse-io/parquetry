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
 * GDAL's Parquet writer emits the GeoParquet 1.1 {@code bbox} covering as float32 columns. A spatial filter lowers to
 * double-valued comparisons on those columns; the reader must promote float to double rather than reject the pair as
 * incompatible. The fixture writes four row groups clustered in distinct regions with a FLOAT {@code bbox} struct, then
 * a {@code BboxIntersects} must prune the disjoint groups at STATS using the covering columns and return exactly the
 * overlapping rows.
 */
class FloatBboxCoveringPruningTest {

    private static final int ROWS_PER_GROUP = 4;
    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath XMIN = ColumnPath.of("bbox", "xmin");
    private static final ColumnPath XMAX = ColumnPath.of("bbox", "xmax");
    private static final ColumnPath YMIN = ColumnPath.of("bbox", "ymin");
    private static final ColumnPath YMAX = ColumnPath.of("bbox", "ymax");

    @TempDir
    Path tempDir;

    @Test
    void intersectsPrunesFloatCoveringAndReturnsOverlappingRows() throws Exception {
        Path file = writeFourClusteredRowGroups();

        ReadOptions options = ReadOptions.DEFAULTS;
        Predicate predicate = Pred.col("geometry").bboxIntersects(Bbox.of2d(0, 0, 10, 10));

        List<PruningDecision> decisions = planDecisions(file, predicate, options);
        List<Integer> ids = readIds(file, predicate, options);

        assertThat(statsEliminations(decisions))
                .as("the three row groups whose float bbox covering is disjoint from the query must be eliminated")
                .isEqualTo(3);
        assertThat(ids)
                .as("only the rows clustered in [0..10]x[0..10] (row group 0) match")
                .containsExactlyInAnyOrder(0, 1, 2, 3);
    }

    private static long statsEliminations(List<PruningDecision> decisions) {
        return decisions.stream()
                .filter(decision -> decision.tier() == Tier.STATS)
                .filter(decision -> decision instanceof PruningDecision.Eliminated)
                .count();
    }

    private Path writeFourClusteredRowGroups() throws Exception {
        ParquetSchema schema = schemaWithFloatBboxCovering();
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .geoParquetMetadata(GeoParquetMetadataMode.V1_1_ONLY)
                .crsEpsg("geometry", 4326)
                .rowGroupSize(RowGroupSize.rows(ROWS_PER_GROUP))
                .build();
        Path file = tempDir.resolve("float-covering.parquet");
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
            double maxX = minX + 1.0;
            double maxY = minY + 1.0;
            String wkt = rectangleWkt(minX, minY, maxX, maxY);
            rows.add(Map.of(
                    ColumnPath.of("geometry"),
                    Wkb.fromWkt(wkt),
                    ID,
                    id,
                    XMIN,
                    (float) minX,
                    XMAX,
                    (float) maxX,
                    YMIN,
                    (float) minY,
                    YMAX,
                    (float) maxY));
        }
        writer.writeBatch(WriteFixtures.batch(schema, rows));
    }

    private static List<Integer> readIds(Path file, Predicate predicate, ReadOptions options) {
        List<Integer> ids = new ArrayList<>();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader dataset = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows = dataset.read(predicate, Projection.ALL, options)) {
                rows.forEach(row -> ids.add(row.getInt(ID)));
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

    private static ParquetSchema schemaWithFloatBboxCovering() {
        SchemaNode.Group bbox = new SchemaNode.Group(
                "bbox",
                Repetition.REQUIRED,
                List.of(requiredFloat("xmin"), requiredFloat("xmax"), requiredFloat("ymin"), requiredFloat("ymax")),
                Optional.empty(),
                -1);
        SchemaNode.Group root = new SchemaNode.Group(
                "schema",
                Repetition.REQUIRED,
                List.of(requiredBinary("geometry"), requiredInt32("id"), bbox),
                Optional.empty(),
                -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredInt32(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive requiredFloat(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.FLOAT, OptionalInt.empty(), Optional.empty(), -1);
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
