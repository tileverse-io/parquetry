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
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.explain.Tier;
import io.tileverse.parquetry.internal.write.BboxCoveringDeriver;
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
 * A GeoParquet 1.1 bbox covering is allowed to enclose each row's geometry rather than match its bounding box edge for
 * edge, and parquetry's own default write does exactly that: for WGS84 data it emits a FLOAT covering whose bounds are
 * rounded outward. A bbox relation answered from those four columns alone therefore answers a box that is up to one
 * float ulp too wide in every direction.
 *
 * <p>These reads pin the answer to the geometry's real bounding box in both directions - no row whose covering overlaps
 * the query while its geometry does not, and no row lost because its covering is wider than the box it was compared
 * against - while the covering keeps eliminating row groups it rules out.
 */
class ConservativeBboxCoveringReadTest {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");

    /** A longitude with no exact float32 representation, which is what makes the covering wider than the geometry. */
    private static final double LINE_X = -58.3816;

    private static final double LINE_MIN_Y = -34.6037;
    private static final double LINE_MAX_Y = -34.5;

    private static final int LINE_ROW = 1;
    private static final int FAR_ROW = 2;

    @TempDir
    Path tempDir;

    @Test
    void queryInsideTheOutwardRoundingGapMatchesNoRow() throws Exception {
        Path file = writeTwoRowGroups();
        Predicate predicate = Pred.col(GEOMETRY).bboxIntersects(roundingGapBox());

        List<Integer> ids = readIds(file, predicate);

        assertThat(ids)
                .as("the query box lies left of the line's real bounding box, inside its outward-rounded covering only")
                .isEmpty();
    }

    @Test
    void equalsOnTheRealBoundingBoxMatchesTheRow() throws Exception {
        Path file = writeTwoRowGroups();
        Predicate predicate = Pred.col(GEOMETRY).bboxEquals(Bbox.of2d(LINE_X, LINE_MIN_Y, LINE_X, LINE_MAX_Y));

        List<Integer> ids = readIds(file, predicate);

        assertThat(ids)
                .as("the query box is the line's real bounding box; the wider covering must not lose the row")
                .containsExactly(LINE_ROW);
    }

    @Test
    void intersectsStillEliminatesTheRowGroupTheCoveringRulesOut() throws Exception {
        Path file = writeTwoRowGroups();
        Predicate predicate = Pred.col(GEOMETRY).bboxIntersects(Bbox.of2d(9.0, 9.0, 12.0, 12.0));

        List<PruningDecision> decisions = planDecisions(file, predicate);
        List<Integer> ids = readIds(file, predicate);

        assertThat(statsEliminations(decisions))
                .as("the row group whose covering is disjoint from the query must still be eliminated at STATS")
                .isEqualTo(1);
        assertThat(ids).containsExactly(FAR_ROW);
    }

    /**
     * A box that ends just short of the line's longitude and starts at the covering's rounded-down xmin: disjoint from
     * the geometry, overlapping the covering.
     */
    private static Bbox roundingGapBox() {
        double coveringMinX = BboxCoveringDeriver.floatFloor(LINE_X);
        assertThat(coveringMinX)
                .as("the fixture needs a longitude that float32 cannot hold exactly")
                .isLessThan(LINE_X);
        return Bbox.of2d(coveringMinX, LINE_MIN_Y, Math.nextDown(LINE_X), LINE_MAX_Y);
    }

    /**
     * Writes the two rows as two row groups through the default geo write, which appends the FLOAT {@code bbox}
     * covering: the vertical line at {@link #LINE_X}, and a unit square far enough away to be pruned on its own.
     */
    private Path writeTwoRowGroups() throws Exception {
        ParquetSchema schema = geometrySchema();
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .crsEpsg("geometry", 4326)
                .rowGroupSize(RowGroupSize.rows(1))
                .build();
        Path file = tempDir.resolve("conservative-covering.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            writeRow(
                    writer,
                    schema,
                    LINE_ROW,
                    "LINESTRING (%s %s, %s %s)".formatted(LINE_X, LINE_MIN_Y, LINE_X, LINE_MAX_Y));
            writeRow(writer, schema, FAR_ROW, "POLYGON ((10 10, 11 10, 11 11, 10 11, 10 10))");
        }
        return file;
    }

    private static void writeRow(ParquetFileWriter writer, ParquetSchema schema, int id, String wkt) {
        Map<ColumnPath, Object> row = Map.of(GEOMETRY, Wkb.fromWkt(wkt), ID, id);
        writer.writeBatch(WriteFixtures.batch(schema, List.of(row)));
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

    private static List<PruningDecision> planDecisions(Path file, Predicate predicate) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            ExplainPlan plan = reader.explain(predicate, Projection.ALL, ReadOptions.DEFAULTS);
            return plan.rowGroups().stream()
                    .flatMap(rowGroup -> rowGroup.tiers().stream())
                    .toList();
        }
    }

    private static long statsEliminations(List<PruningDecision> decisions) {
        return decisions.stream()
                .filter(decision -> decision.tier() == Tier.STATS)
                .filter(PruningDecision.Eliminated.class::isInstance)
                .count();
    }

    private static ParquetSchema geometrySchema() {
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
