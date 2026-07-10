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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.EdgeInterpolationAlgorithm;
import io.tileverse.parquetry.schema.geo.ParquetCrs;

class IcebergFieldTest {

    @Test
    void defaultsGeoComponentsToEmpty() {
        IcebergField field = new IcebergField(1, "g", "geometry", false);
        assertThat(field.initialDefault()).isEmpty();
        assertThat(field.crs()).isEmpty();
        assertThat(field.geographyAlgorithm()).isEmpty();
    }

    @Test
    void exposesGeoComponentsFromFullConstructor() {
        Optional<ParquetCrs> crs = Optional.of(new ParquetCrs.AuthorityCode("EPSG", "3857"));
        Optional<EdgeInterpolationAlgorithm> algorithm = Optional.of(EdgeInterpolationAlgorithm.KARNEY);
        IcebergField field = new IcebergField(2, "h", "geography", false, Optional.empty(), crs, algorithm);
        assertThat(field.crs()).isEqualTo(crs);
        assertThat(field.geographyAlgorithm()).isEqualTo(algorithm);
        assertThat(field.isGeography()).isTrue();
    }
}
