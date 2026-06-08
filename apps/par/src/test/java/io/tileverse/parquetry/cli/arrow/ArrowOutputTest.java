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

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.cli.expr.FilterParser;
import io.tileverse.parquetry.cli.expr.GeometryColumns;
import io.tileverse.parquetry.cli.render.Projections;
import io.tileverse.parquetry.cli.support.Fixtures;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

class ArrowOutputTest {

    @Test
    void fullReadNoFilter(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            ParquetDataset dataset = ParquetDataset.open(channel);
            ParquetSchema schema = dataset.schema();
            ArrowOutputRequest request =
                    new ArrowOutputRequest(Predicate.ALWAYS_TRUE, Projection.ALL, false, Long.MAX_VALUE);
            ArrowOutput.write(dataset, schema, Optional.empty(), request, out);
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
            ParquetDataset dataset = ParquetDataset.open(channel);
            ParquetSchema schema = dataset.schema();
            Set<ColumnPath> geometryColumns = GeometryColumns.resolve(schema, dataset.keyValueMetadata());
            Predicate predicate = FilterParser.parse("pop > 1300000", schema, geometryColumns);
            ArrowOutputRequest request = new ArrowOutputRequest(predicate, Projection.ALL, true, Long.MAX_VALUE);
            ArrowOutput.write(dataset, schema, Optional.empty(), request, out);
        }

        assertThat(rowCount(out.toByteArray())).isEqualTo(2);
    }

    @Test
    void limitTrims(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            ParquetDataset dataset = ParquetDataset.open(channel);
            ParquetSchema schema = dataset.schema();
            ArrowOutputRequest request = new ArrowOutputRequest(Predicate.ALWAYS_TRUE, Projection.ALL, false, 2);
            ArrowOutput.write(dataset, schema, Optional.empty(), request, out);
        }

        assertThat(rowCount(out.toByteArray())).isEqualTo(2);
    }

    @Test
    void projectionKeepsOnlyRequested(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            ParquetDataset dataset = ParquetDataset.open(channel);
            ParquetSchema schema = dataset.schema();
            Projections.Resolved resolved = Projections.resolve(List.of("id", "name"), schema);
            ParquetSchema projectedSchema = schema.project(Set.copyOf(resolved.keptLeaves()));
            ArrowOutputRequest request =
                    new ArrowOutputRequest(Predicate.ALWAYS_TRUE, resolved.projection(), false, Long.MAX_VALUE);
            ArrowOutput.write(dataset, projectedSchema, Optional.empty(), request, out);
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
            ParquetDataset dataset = ParquetDataset.open(channel);
            ParquetSchema schema = dataset.schema();
            Optional<GeoParquetMetadata> geo = geoMetadata(dataset);
            ArrowOutputRequest request =
                    new ArrowOutputRequest(Predicate.ALWAYS_TRUE, Projection.ALL, false, Long.MAX_VALUE);
            ArrowOutput.write(dataset, schema, geo, request, out);
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

    private static Optional<GeoParquetMetadata> geoMetadata(ParquetDataset dataset) {
        String geoJson = dataset.keyValueMetadata().get("geo");
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
