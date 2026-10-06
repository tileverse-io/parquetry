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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.SchemaElement;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.testsupport.Wkb;

/**
 * A column without a defined order has no min or max, and its chunks get an offset index and no column index: the
 * format defines the page bounds of a column index by the order of the column. A geometry column is such a column,
 * whether a chunk holds values or nulls only.
 */
class UnorderedColumnIndexTest {

    private static final String GEOMETRY_COLUMN = "geometry";
    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath GEOMETRY = ColumnPath.of(GEOMETRY_COLUMN);
    private static final ColumnPath NAME = ColumnPath.of("name");
    private static final int POINTS = 300;
    private static final int POINTS_PER_PAGE = 100;
    private static final int WGS84 = 4326;

    @TempDir
    Path tempDir;

    @Test
    void geometryChunkHasAnOffsetIndexAndNoColumnIndex() throws IOException {
        FileMetaData footer = footerOf(writePoints());

        assertThat(geometryLeaf(footer).logicalType()).containsInstanceOf(LogicalType.Geometry.class);
        ColumnChunk geometry = chunkOf(footer, GEOMETRY);
        assertThat(geometry.offsetIndexOffset()).as("geometry offset index").isPresent();
        assertThat(geometry.columnIndexOffset()).as("geometry column index").isEmpty();
        assertThat(geometry.columnIndexLength())
                .as("geometry column index length")
                .isEmpty();
    }

    @Test
    void chunkOfOnlyNullGeometriesHasNoColumnIndex() throws IOException {
        FileMetaData footer = footerOf(writeNulls());

        ColumnChunk geometry = chunkOf(footer, GEOMETRY);
        assertThat(geometry.offsetIndexOffset()).as("geometry offset index").isPresent();
        assertThat(geometry.columnIndexOffset()).as("geometry column index").isEmpty();
    }

    @Test
    void orderedColumnOfOnlyNullsKeepsItsColumnIndex() throws IOException {
        FileMetaData footer = footerOf(writeNulls());

        ColumnChunk name = chunkOf(footer, NAME);
        assertThat(name.columnIndexOffset()).as("name column index").isPresent();
    }

    @Test
    void orderedColumnOfTheSameFileKeepsItsColumnIndex() throws IOException {
        FileMetaData footer = footerOf(writePoints());

        ColumnChunk id = chunkOf(footer, ID);
        assertThat(id.offsetIndexOffset()).as("id offset index").isPresent();
        assertThat(id.columnIndexOffset()).as("id column index").isPresent();
    }

    @Test
    void nullFiltersOnTheGeometryStillRead() throws IOException {
        Path file = writePoints();

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            assertThat(reader.count(new Predicate.IsNotNull(GEOMETRY), ReadOptions.DEFAULTS))
                    .isEqualTo(POINTS - POINTS_PER_PAGE);
            assertThat(reader.count(new Predicate.IsNull(GEOMETRY), ReadOptions.DEFAULTS))
                    .isEqualTo(POINTS_PER_PAGE);
        }
    }

    private static FileMetaData footerOf(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            return ParquetFormat.readFooter(source);
        }
    }

    private static SchemaElement geometryLeaf(FileMetaData footer) {
        return footer.schema().stream()
                .filter(element -> element.name().equals(GEOMETRY_COLUMN))
                .findFirst()
                .orElseThrow();
    }

    private static ColumnChunk chunkOf(FileMetaData footer, ColumnPath column) {
        return footer.rowGroups().getFirst().columns().stream()
                .filter(chunk -> ColumnPath.of(chunk.metaData().orElseThrow().pathInSchema())
                        .equals(column))
                .findFirst()
                .orElseThrow();
    }

    /** Three pages of points in one row group; the middle page holds only null geometries. */
    private Path writePoints() throws IOException {
        ParquetSchema schema = WriteFixtures.schemaOf(
                WriteFixtures.requiredLeaf("id", PrimitiveKind.INT32),
                WriteFixtures.optionalLeaf(GEOMETRY_COLUMN, PrimitiveKind.BYTE_ARRAY));
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (int id = 0; id < POINTS; id++) {
            rows.add(pointRow(id));
        }
        return WriteFixtures.writeRows(tempDir.resolve("points.parquet"), schema, options(), rows);
    }

    /** A row names its geometry unless it falls in the page of nulls. */
    private static Map<ColumnPath, Object> pointRow(int id) {
        boolean inNullPage = id / POINTS_PER_PAGE == 1;
        if (inNullPage) {
            return Map.of(ID, id);
        }
        return Map.of(ID, id, GEOMETRY, Wkb.fromWkt("POINT (" + id + " " + -id + ")"));
    }

    /** One row group with nothing but nulls in its geometry and name columns. */
    private Path writeNulls() throws IOException {
        ParquetSchema schema = WriteFixtures.schemaOf(
                WriteFixtures.requiredLeaf("id", PrimitiveKind.INT32),
                WriteFixtures.optionalLeaf(GEOMETRY_COLUMN, PrimitiveKind.BYTE_ARRAY),
                WriteFixtures.optionalLeaf("name", PrimitiveKind.BYTE_ARRAY));
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (int id = 0; id < POINTS; id++) {
            rows.add(Map.of(ID, id));
        }
        return WriteFixtures.writeRows(tempDir.resolve("nulls.parquet"), schema, options(), rows);
    }

    private WriteOptions options() {
        return WriteOptions.builder()
                .tempDir(tempDir)
                .pageValueLimit(POINTS_PER_PAGE)
                .crsEpsg(GEOMETRY_COLUMN, WGS84)
                .build();
    }
}
