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
package io.tileverse.parquetry.cli.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.cli.expr.FilterParser;
import io.tileverse.parquetry.cli.render.Projections;
import io.tileverse.parquetry.cli.support.Fixtures;
import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.schema.geo.geoparquet.GeometryColumns;

class ArrowOutputTest {

    @Test
    void fullReadNoFilter(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            ParquetSource source = ParquetSource.open(channel);
            ParquetSchema schema = source.schema();
            ArrowOutputRequest request = new ArrowOutputRequest(Predicate.ALWAYS_TRUE, Projection.ALL, Long.MAX_VALUE);
            ArrowOutput.write(source, schema, Optional.empty(), request, out);
        }

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader =
                        new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
            int rows = 0;
            String firstName = null;
            long firstPop = 0;
            boolean readFirstRow = false;
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                if (!readFirstRow && root.getRowCount() > 0) {
                    VarCharVector name = (VarCharVector) root.getVector("name");
                    BigIntVector pop = (BigIntVector) root.getVector("pop");
                    firstName = new String(name.get(0));
                    firstPop = pop.get(0);
                    readFirstRow = true;
                }
                rows += root.getRowCount();
            }
            assertThat(rows).isEqualTo(4);
            assertThat(firstName).isEqualTo("Rosario");
            assertThat(firstPop).isEqualTo(1_300_000L);
        }
    }

    @Test
    void filteredIsExact(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            ParquetSource source = ParquetSource.open(channel);
            ParquetSchema schema = source.schema();
            Set<ColumnPath> geometryColumns = GeometryColumns.resolve(schema, source.keyValueMetadata());
            Predicate predicate = FilterParser.parse("pop > 1300000", schema, geometryColumns);
            ArrowOutputRequest request = new ArrowOutputRequest(predicate, Projection.ALL, Long.MAX_VALUE);
            ArrowOutput.write(source, schema, Optional.empty(), request, out);
        }

        assertThat(rowCount(out.toByteArray())).isEqualTo(2);
    }

    /** The cities fixture holds 4 rows in one batch; the vectors cover zero, mid-batch, boundary, and beyond. */
    @ParameterizedTest(name = "limit {0} emits {1} rows")
    @MethodSource("limitCases")
    void limitCapsEmittedRows(long limit, int expectedRows, @TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            ParquetSource source = ParquetSource.open(channel);
            ArrowOutputRequest request = new ArrowOutputRequest(Predicate.ALWAYS_TRUE, Projection.ALL, limit);
            ArrowOutput.write(source, source.schema(), Optional.empty(), request, out);
        }

        assertThat(rowCount(out.toByteArray())).isEqualTo(expectedRows);
    }

    static Stream<Arguments> limitCases() {
        return Stream.of(Arguments.of(0L, 0), Arguments.of(2L, 2), Arguments.of(4L, 4), Arguments.of(99L, 4));
    }

    @Test
    void filteredWithLimitCapsExactly(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            ParquetSource source = ParquetSource.open(channel);
            ParquetSchema schema = source.schema();
            Set<ColumnPath> geometryColumns = GeometryColumns.resolve(schema, source.keyValueMetadata());
            Predicate predicate = FilterParser.parse("pop > 1300000", schema, geometryColumns);
            ArrowOutputRequest request = new ArrowOutputRequest(predicate, Projection.ALL, 1);
            ArrowOutput.write(source, schema, Optional.empty(), request, out);
        }

        assertThat(rowCount(out.toByteArray())).isEqualTo(1);
    }

    @Test
    void projectionKeepsOnlyRequested(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            ParquetSource source = ParquetSource.open(channel);
            ParquetSchema schema = source.schema();
            Projections.Resolved resolved = Projections.resolve(List.of("id", "name"), schema);
            ParquetSchema projectedSchema = schema.project(Set.copyOf(resolved.keptLeaves()));
            ArrowOutputRequest request =
                    new ArrowOutputRequest(Predicate.ALWAYS_TRUE, resolved.projection(), Long.MAX_VALUE);
            ArrowOutput.write(source, projectedSchema, Optional.empty(), request, out);
        }

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader =
                        new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            List<String> fieldNames =
                    root.getSchema().getFields().stream().map(Field::getName).toList();
            assertThat(fieldNames).containsExactly("id", "name");
        }
    }

    @Test
    void geometryFieldHasGeoArrowExtension(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("geocities.parquet");
        Fixtures.writeGeoCities(file);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            ParquetSource source = ParquetSource.open(channel);
            ParquetSchema schema = source.schema();
            Optional<GeoParquetMetadata> geo = geoMetadata(source);
            ArrowOutputRequest request = new ArrowOutputRequest(Predicate.ALWAYS_TRUE, Projection.ALL, Long.MAX_VALUE);
            ArrowOutput.write(source, schema, geo, request, out);
        }

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader =
                        new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            Field geometry = root.getSchema().findField("geometry");
            assertThat(geometry.getMetadata()).containsEntry("ARROW:extension:name", "geoarrow.wkb");
        }
    }

    private static Optional<GeoParquetMetadata> geoMetadata(ParquetSource source) {
        String geoJson = source.keyValueMetadata().get("geo");
        if (geoJson == null || geoJson.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(GeoParquetMetadata.parse(geoJson));
    }

    private static int rowCount(byte[] bytes) throws Exception {
        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)) {
            int rows = 0;
            while (reader.loadNextBatch()) {
                rows += reader.getVectorSchemaRoot().getRowCount();
            }
            return rows;
        }
    }
}
