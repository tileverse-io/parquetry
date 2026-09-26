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
package io.tileverse.parquetry.geotools.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;

import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;

import io.tileverse.parquetry.geotools.data.FeatureTypeMapper.AttributeMapping;
import io.tileverse.parquetry.geotools.data.FeatureTypeMapper.Mapping;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Pins which reads may take a substitute: the primary geometry alone, or the primary geometry and the feature id
 * column.
 */
class CatalogFeatureSourceGeometryOnlyTest {

    private static final AttributeMapping GEOMETRY =
            new AttributeMapping("geometry", ColumnPath.of("geometry"), true, Geometry.class);
    private static final AttributeMapping ID = new AttributeMapping("id", ColumnPath.of("id"), false, Long.class);
    private static final AttributeMapping NAME =
            new AttributeMapping("name", ColumnPath.of("name"), false, String.class);

    @Test
    void theGeometryAloneIsGeometryOnly() {
        Mapping mapping = new Mapping(featureType(), List.of(GEOMETRY, ID, NAME));

        assertThat(CatalogFeatureSource.geometryOnly(mapping, List.of(GEOMETRY)))
                .isTrue();
    }

    @Test
    void theGeometryWithTheFeatureIdColumnIsGeometryOnly() {
        Mapping mapping = new Mapping(featureType(), List.of(GEOMETRY, ID, NAME), Optional.of(ID));

        assertThat(CatalogFeatureSource.geometryOnly(mapping, List.of(GEOMETRY, ID)))
                .isTrue();
    }

    @Test
    void anyOtherAttributeIsNot() {
        Mapping withoutFid = new Mapping(featureType(), List.of(GEOMETRY, ID, NAME));
        Mapping withFid = new Mapping(featureType(), List.of(GEOMETRY, ID, NAME), Optional.of(ID));

        assertThat(CatalogFeatureSource.geometryOnly(withoutFid, List.of(GEOMETRY, ID)))
                .isFalse();
        assertThat(CatalogFeatureSource.geometryOnly(withFid, List.of(GEOMETRY, ID, NAME)))
                .isFalse();
    }

    @Test
    void aReadWithoutTheGeometryIsNot() {
        Mapping mapping = new Mapping(featureType(), List.of(GEOMETRY, ID, NAME), Optional.of(ID));

        assertThat(CatalogFeatureSource.geometryOnly(mapping, List.of(ID))).isFalse();
        assertThat(CatalogFeatureSource.geometryOnly(mapping, List.of())).isFalse();
    }

    private static SimpleFeatureType featureType() {
        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        builder.setName("points");
        builder.add("geometry", Geometry.class);
        builder.add("id", Long.class);
        builder.add("name", String.class);
        builder.setDefaultGeometry("geometry");
        return builder.buildFeatureType();
    }
}
