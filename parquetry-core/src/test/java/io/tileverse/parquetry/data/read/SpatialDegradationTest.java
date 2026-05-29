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
package io.tileverse.parquetry.data.read;

import static org.assertj.core.api.Assertions.assertThat;

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

import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteRow;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.Wkb;

/**
 * Guard tests for graceful degradation of bbox spatial predicates and {@link Predicate.Not} wrapping spatial leaves.
 *
 * <p>Three scenarios are exercised:
 *
 * <ol>
 *   <li>Interleaved bounding boxes that prevent any row-group elimination: record-level WKB evaluation still yields the
 *       exact survivor set.
 *   <li>{@code Not(bboxCoveredBy(q))} returns the complement at record level; the spatial bounds evaluator
 *       conservatively keeps every row group.
 *   <li>A non-spatial predicate on a plain INT32 column is unaffected by the spatial machinery.
 * </ol>
 */
class SpatialDegradationTest {

    @TempDir
    Path tempDir;

    /**
     * Writes eight rectangles whose union bbox spans the entire query region. The row-group pruner cannot eliminate any
     * row group; the record-level WKB evaluator must produce the exact intersecting subset.
     *
     * <p>Fixture (id, rect bbox):
     *
     * <ul>
     *   <li>id=0: [0,0 - 3,3]
     *   <li>id=1: [7,7 - 10,10]
     *   <li>id=2: [0,7 - 3,10]
     *   <li>id=3: [7,0 - 10,3]
     *   <li>id=4: [4,4 - 6,6] - overlaps query
     *   <li>id=5: [3,3 - 7,7] - overlaps query (edge inclusive)
     *   <li>id=6: [11,11 - 13,13] - disjoint from query
     *   <li>id=7: [0,0 - 10,10] - entirely covers query
     * </ul>
     *
     * <p>Query bbox: [4,4 - 8,8]. The union of all rects' bboxes covers [0,0 - 13,13], which overlaps the query; the
     * pruner cannot drop the row group. Record-level WKB evaluation determines the survivors.
     */
    @Test
    void spatiallySpreadDataReturnsExactRowsEvenWhenNoRowGroupIsEliminated() throws Exception {
        ParquetSchema schema = flatSchema(requiredBinary("geometry"), requiredInt32("id"));
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .crsEpsg("geometry", 4326)
                .build();
        Path file = tempDir.resolve("spread.parquet");
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.write(geoRow(0, 0, 0, 3, 3)); // disjoint from query [4,4-8,8]
            writer.write(geoRow(1, 7, 7, 10, 10)); // overlaps query on SE corner
            writer.write(geoRow(2, 0, 7, 3, 10)); // disjoint from query
            writer.write(geoRow(3, 7, 0, 10, 3)); // disjoint from query
            writer.write(geoRow(4, 4, 4, 6, 6)); // inside query, intersects
            writer.write(geoRow(5, 3, 3, 7, 7)); // overlaps query
            writer.write(geoRow(6, 11, 11, 13, 13)); // completely disjoint
            writer.write(geoRow(7, 0, 0, 10, 10)); // covers query entirely
        }

        // Union of all bboxes is [0,0-13,13], which overlaps [4,4-8,8] -> no row group eliminated.
        // Record-level: id=0 [0,0-3,3] is disjoint (maxX=3 < 4=q.minX); id=2 [0,7-3,10] disjoint (maxX=3 < 4)
        // id=3 [7,0-10,3] disjoint (maxY=3 < 4=q.minY); id=6 [11,11-13,13] disjoint.
        // Survivors: id=1,4,5,7.
        List<Integer> ids = readIds(file, Pred.col("geometry").bboxIntersects(Bbox.of2d(4, 4, 8, 8)));
        assertThat(ids)
                .as("bboxIntersects with no row-group elimination must match exactly at record level")
                .containsExactlyInAnyOrder(1, 4, 5, 7);
    }

    /**
     * Proves that wrapping a spatial leaf in {@link Predicate.Not} returns the complement at record level.
     *
     * <p>Fixture (id, rect bbox):
     *
     * <ul>
     *   <li>id=0: [1,1 - 3,3] - covered by query [0,0-5,5]
     *   <li>id=1: [0,0 - 5,5] - equal to query, edges inclusive -> covered
     *   <li>id=2: [4,4 - 8,8] - escapes query (maxX=8 > 5)
     *   <li>id=3: [6,6 - 9,9] - entirely outside query
     *   <li>id=4: [-1,2 - 3,4] - escapes query (minX=-1 < 0)
     * </ul>
     *
     * <p>Query bbox for bboxCoveredBy: [0,0 - 5,5]. Covered ids: 0, 1. Not-covered ids: 2, 3, 4.
     */
    @Test
    void notBboxCoveredByReturnsTheComplementAtRecordLevel() throws Exception {
        ParquetSchema schema = flatSchema(requiredBinary("geometry"), requiredInt32("id"));
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .crsEpsg("geometry", 4326)
                .build();
        Path file = tempDir.resolve("not_covered.parquet");
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.write(geoRow(0, 1, 1, 3, 3)); // covered by q
            writer.write(geoRow(1, 0, 0, 5, 5)); // equal to q -> covered
            writer.write(geoRow(2, 4, 4, 8, 8)); // escapes (maxX=8 > 5)
            writer.write(geoRow(3, 6, 6, 9, 9)); // entirely outside q
            writer.write(geoRow(4, -1, 2, 3, 4)); // escapes (minX=-1 < 0)
        }

        Predicate notCoveredBy = new Predicate.Not(Pred.col("geometry").bboxCoveredBy(Bbox.of2d(0, 0, 5, 5)));
        List<Integer> ids = readIds(file, notCoveredBy);
        assertThat(ids)
                .as("Not(bboxCoveredBy) must return exactly the rows not covered by the query box")
                .containsExactlyInAnyOrder(2, 3, 4);
    }

    /**
     * Proves that a non-spatial predicate on a plain integer column is unaffected by the spatial filter machinery. The
     * file has no geometry column; only INT32 columns are present.
     *
     * <p>Fixture (id, value): 0..9 with value == id. Predicate: value > 5. Expected survivors: ids 6,7,8,9 (value == id
     * > 5).
     */
    @Test
    void nonGeometryReadIsUnaffected() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"), requiredInt32("value"));
        WriteOptions options = WriteOptions.builder().tempDir(tempDir).build();
        Path file = tempDir.resolve("plain.parquet");
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            for (int i = 0; i < 10; i++) {
                writer.write(WriteRow.of(Map.of(ColumnPath.of("id"), i, ColumnPath.of("value"), i)));
            }
        }

        List<Integer> ids = readIds(file, Pred.col("value").gt(5));
        assertThat(ids)
                .as("plain integer predicate must work when there is no geometry column")
                .containsExactlyInAnyOrder(6, 7, 8, 9);
    }

    // --- helpers ---

    private List<Integer> readIds(Path file, Predicate predicate) {
        List<Integer> ids = new ArrayList<>();
        try (ByteRangeSource src = ByteRangeSource.ofFile(file)) {
            ParquetDataset ds = ParquetDataset.open(src);
            try (Stream<ParquetRecord> rows = ds.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
                rows.forEach(r -> ids.add(r.getInt(ColumnPath.of("id"))));
            }
        }
        return ids;
    }

    private static WriteRow geoRow(int id, double minX, double minY, double maxX, double maxY) {
        MemorySegment geom = Wkb.fromWkt(rectangleWkt(minX, minY, maxX, maxY));
        return WriteRow.of(Map.of(ColumnPath.of("geometry"), geom, ColumnPath.of("id"), id));
    }

    /** Closed-ring rectangular polygon in WKT, corners walked counter-clockwise: SW -> SE -> NE -> NW -> SW. */
    private static String rectangleWkt(double minX, double minY, double maxX, double maxY) {
        return "POLYGON ((%s %s, %s %s, %s %s, %s %s, %s %s))"
                .formatted(minX, minY, maxX, minY, maxX, maxY, minX, maxY, minX, minY);
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
}
