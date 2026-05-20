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
package io.tileverse.parquetry.geotools;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;

import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.GeometryDescriptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Geometry;

import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.testkit.TestCorpus;

class GeoParquetSchemaMapperTest {

    @Test
    void mapsExampleGeoParquetToSimpleFeatureType(@TempDir Path dir) throws Exception {
        Path file = TestCorpus.extractFile("geoparquet/examples/example.parquet", dir);
        try (ByteRangeSource src = ByteRangeSource.ofFile(file)) {
            ParquetDataset ds = ParquetDataset.open(src);
            GeoParquetSchemaMapper.Mapping mapping =
                    GeoParquetSchemaMapper.map("example", null, ds.schema(), ds.keyValueMetadata());
            SimpleFeatureType ft = mapping.featureType();

            GeometryDescriptor geom = ft.getGeometryDescriptor();
            assertThat(geom).isNotNull();
            assertThat(geom.getType().getBinding()).isAssignableFrom(Geometry.class);
            assertThat(ft.getCoordinateReferenceSystem()).isNotNull();

            assertThat(ft.getAttributeDescriptors())
                    .extracting(d -> d.getLocalName())
                    .contains("name");
            assertThat(mapping.attributes()).isNotEmpty();
            assertThat(mapping.attributes())
                    .allSatisfy(a -> assertThat(a.path()).isNotNull());
        }
    }
}
