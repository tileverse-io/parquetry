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
package io.tileverse.parquetry.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.testkit.TestCorpus;

class GeometryRoundTripTest {

    @Test
    void wkbGeometryRoundTripsWithGeoArrowExtension(@TempDir Path dir) throws Exception {
        Path file = TestCorpus.extractFile("geoparquet/test_data/data-polygon-encoding_wkb.parquet", dir);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            ParquetDataset dataset = ParquetDataset.open(channel);
            Optional<GeoParquetMetadata> geo = Optional.empty();
            String geoJson = dataset.keyValueMetadata().get("geo");
            if (geoJson != null && !geoJson.isEmpty()) {
                geo = Optional.of(GeoParquetMetadata.parse(geoJson));
            }
            try (Stream<ParquetRecordBatch> batches =
                    dataset.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                ArrowIpcWriter.write(dataset.schema(), geo, batches, out);
            }
        }

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader =
                        new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertThat(root.getRowCount()).isPositive();

            Field geometryField = null;
            for (Field field : root.getSchema().getFields()) {
                if ("geoarrow.wkb".equals(field.getMetadata().get("ARROW:extension:name"))) {
                    geometryField = field;
                }
            }
            assertThat(geometryField)
                    .as("expected a field tagged with the geoarrow.wkb extension")
                    .isNotNull();
        }
    }
}
