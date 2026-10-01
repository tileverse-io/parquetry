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
package io.tileverse.parquetry.internal.write;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.Wkb;

/**
 * Proves the covering page value limit reaches the written file: the derived {@code bbox} leaves split at the requested
 * value count while the geometry column keeps the pages it would have had, which is what makes a row group's spatial
 * plan finer without re-encoding the geometry.
 */
class CoveringPageValueLimitWriteTest {

    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");
    private static final ColumnPath XMIN = ColumnPath.of("bbox", "xmin");
    private static final int ROWS = 2_048;

    @TempDir
    Path tempDir;

    @Test
    void aCoveringLimitSplitsOnlyTheCoveringLeaves() throws Exception {
        Path file = writePoints("covering-512", builder -> builder.coveringPageValueLimit(512));

        assertThat(pageCount(file, XMIN))
                .as("%d rows at 512 values a page", ROWS)
                .isEqualTo(ROWS / 512);
        assertThat(pageCount(file, GEOMETRY))
                .as(
                        "the geometry column keeps one page: %d values under the shared limit and under the byte limit",
                        ROWS)
                .isEqualTo(1);
    }

    @Test
    void withoutACoveringLimitEveryColumnKeepsOnePage() throws Exception {
        Path file = writePoints("covering-default", builder -> builder);

        assertThat(pageCount(file, XMIN)).isEqualTo(1);
        assertThat(pageCount(file, GEOMETRY)).isEqualTo(1);
    }

    /** The number of data pages written for {@code column} in the file's single row group. */
    private static int pageCount(Path file, ColumnPath column) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            RowGroup rowGroup = footer.rowGroups().getFirst();
            for (ColumnChunk chunk : rowGroup.columns()) {
                ColumnMetaData meta = chunk.metaData().orElseThrow();
                if (!column.equals(ColumnPath.of(meta.pathInSchema()))) {
                    continue;
                }
                long offset = chunk.offsetIndexOffset().orElseThrow();
                int length = chunk.offsetIndexLength().orElseThrow();
                return ParquetFormat.readOffsetIndex(source, offset, length)
                        .pageLocations()
                        .size();
            }
            throw new IllegalStateException("no column chunk for " + column.dot());
        }
    }

    private Path writePoints(String name, java.util.function.UnaryOperator<WriteOptions.Builder> tuning)
            throws Exception {
        ParquetSchema schema = geometryOnlySchema();
        WriteOptions.Builder builder = WriteOptions.builder()
                .tempDir(tempDir)
                .crsEpsg("geometry", 4326)
                .pageValueLimit(8_192);
        WriteOptions options = tuning.apply(builder).build();
        Path file = tempDir.resolve(name + ".parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.writeBatch(WriteFixtures.batch(schema, marchingPointRows()));
        }
        return file;
    }

    private static List<Map<ColumnPath, Object>> marchingPointRows() {
        List<Map<ColumnPath, Object>> rows = new ArrayList<>(ROWS);
        for (int id = 0; id < ROWS; id++) {
            String wkt = "POINT (%d 5)".formatted(id);
            rows.add(Map.of(GEOMETRY, Wkb.fromWkt(wkt)));
        }
        return rows;
    }

    private static ParquetSchema geometryOnlySchema() {
        SchemaNode.Primitive geometry = new SchemaNode.Primitive(
                "geometry", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(geometry), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
