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
package io.tileverse.parquetry.geotools.export;

import static org.assertj.core.api.Assertions.assertThat;

import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Point;

import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Pins the contract that {@code ArrowIpcWriter}'s geometry detection (in the {@code parquetry-arrow} module, not on
 * this module's classpath) relies on: for a leaf with no {@code Geometry} logical type, the detector looks up
 * {@code geo.columns().get(path.dot())} and reads that {@link GeoColumn}'s {@code crs()} / {@code edges()}. This test
 * exists as a regression pin on the write side of that contract, not to exercise new behavior.
 */
class FeatureRecordBatchesGeoMetadataTest {

    @Test
    void geoMetadataMatchesTheArrowDetectionContract() throws Exception {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("t");
        typeBuilder.setCRS(CRS.decode("EPSG:4326", true));
        typeBuilder.add("geom", Point.class);
        SimpleFeatureType featureType = typeBuilder.buildFeatureType();

        FeatureRecordBatches recordBatches = FeatureRecordBatches.forType(featureType);

        GeoParquetMetadata geoMetadata = recordBatches.geoMetadata().orElseThrow();
        assertThat(geoMetadata).isInstanceOf(GeoParquetMetadata.V1_1.class);
        assertThat(geoMetadata.primaryColumn()).isEqualTo("geom");

        GeoColumn geomColumn = geoMetadata.columns().get("geom");
        assertThat(geomColumn).isNotNull();
        assertThat(geomColumn.encoding()).contains("WKB");
        assertThat(geomColumn.crs()).isPresent();
        assertThat(geomColumn.edges())
                .as("no edges entry is written; the Arrow side reads edges().orElse(\"planar\")")
                .isEmpty();

        SchemaNode geomLeaf =
                recordBatches.parquetSchema().find(ColumnPath.of("geom")).orElseThrow();
        assertThat(geomLeaf).isInstanceOf(SchemaNode.Primitive.class);
        assertThat(geomLeaf.logicalType()).isEmpty();
    }
}
