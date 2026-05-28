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

import org.junit.jupiter.api.BeforeEach;
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
 * End-to-end proof that the record-level filter evaluates bbox spatial relations from each row's WKB geometry. Five
 * rectangle polygons with distinct bboxes are written to a single GeoParquet file; each test reads with one relation
 * and asserts the exact surviving row ids.
 *
 * <p>Fixture rows (id, rectangle bbox):
 *
 * <ul>
 *   <li>id=0: [0,0 - 2,2]
 *   <li>id=1: [5,5 - 7,7]
 *   <li>id=2: [0,0 - 10,10]
 *   <li>id=3: [4,4 - 9,9]
 *   <li>id=4: [12,12 - 14,14]
 * </ul>
 */
class SpatialRecordLevelReadTest {

    @TempDir
    Path tempDir;

    private Path fixtureFile;

    @BeforeEach
    void writeFixture() throws Exception {
        ParquetSchema schema = flatSchema(requiredBinary("geometry"), requiredInt32("id"));
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .crsEpsg("geometry", 4326)
                .build();
        fixtureFile = tempDir.resolve("spatial.parquet");
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(fixtureFile), schema, options)) {
            writer.write(row(0, 0, 0, 2, 2));
            writer.write(row(1, 5, 5, 7, 7));
            writer.write(row(2, 0, 0, 10, 10));
            writer.write(row(3, 4, 4, 9, 9));
            writer.write(row(4, 12, 12, 14, 14));
        }
    }

    @Test
    void bboxIntersectsKeepsRowsWhoseBboxOverlapsQuery() {
        // Query [4,4 - 9,9]: row 0 ([0,0-2,2]) is disjoint; row 4 ([12,12-14,14]) is disjoint.
        List<Integer> ids = readIds(Pred.col("geometry").bboxIntersects(Bbox.of2d(4, 4, 9, 9)));
        assertThat(ids).as("bboxIntersects").containsExactlyInAnyOrder(1, 2, 3);
    }

    @Test
    void bboxContainsKeepsRowsWhoseBboxEnclosesQuery() {
        // Query [5,5 - 7,7]: rows whose bbox encloses [5,5-7,7] (edges inclusive).
        // id=0 [0,0-2,2] does not enclose [5,5-7,7]; id=4 [12,12-14,14] does not; id=1 [5,5-7,7] equals -> contains.
        List<Integer> ids = readIds(Pred.col("geometry").bboxContains(Bbox.of2d(5, 5, 7, 7)));
        assertThat(ids).as("bboxContains").containsExactlyInAnyOrder(1, 2, 3);
    }

    @Test
    void bboxCoveredByKeepsRowsWhoseBboxFitsInsideQuery() {
        // Query [0,0 - 10,10]: row 4 [12,12-14,14] escapes; rows 0/1/2/3 all fit (row 2 equals, edges inclusive).
        List<Integer> ids = readIds(Pred.col("geometry").bboxCoveredBy(Bbox.of2d(0, 0, 10, 10)));
        assertThat(ids).as("bboxCoveredBy").containsExactlyInAnyOrder(0, 1, 2, 3);
    }

    @Test
    void bboxEqualsKeepsOnlyRowWhoseBboxMatchesExactly() {
        // Only id=2 has the exact bbox [0,0 - 10,10].
        List<Integer> ids = readIds(Pred.col("geometry").bboxEquals(Bbox.of2d(0, 0, 10, 10)));
        assertThat(ids).as("bboxEquals").containsExactlyInAnyOrder(2);
    }

    // --- helpers ---

    private List<Integer> readIds(Predicate predicate) {
        List<Integer> ids = new ArrayList<>();
        try (ByteRangeSource src = ByteRangeSource.ofFile(fixtureFile)) {
            ParquetDataset ds = ParquetDataset.open(src);
            try (Stream<ParquetRecord> rows = ds.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
                rows.forEach(r -> ids.add(r.getInt(ColumnPath.of("id"))));
            }
        }
        return ids;
    }

    private static WriteRow row(int id, double minX, double minY, double maxX, double maxY) {
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
