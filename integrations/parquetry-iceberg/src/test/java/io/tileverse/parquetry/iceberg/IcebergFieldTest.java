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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.EdgeInterpolationAlgorithm;
import io.tileverse.parquetry.schema.geo.ParquetCrs;

class IcebergFieldTest {

    @Test
    void parsesTheTokenThroughTheConvenienceConstructor() {
        IcebergField field = new IcebergField(1, "amount", "decimal(9, 2)", false);
        assertThat(field.type()).isEqualTo(new IcebergType.DecimalType(9, 2));
        assertThat(field.initialDefault()).isEmpty();
    }

    @Test
    void reportsGeometryAndGeographyFromTheType() {
        IcebergField geometry = new IcebergField(1, "g", "geometry(EPSG:3857)", false);
        assertThat(geometry.isGeometry()).isTrue();
        assertThat(geometry.isGeography()).isFalse();

        IcebergField geography = new IcebergField(2, "h", "geography(OGC:CRS84, karney)", false);
        assertThat(geography.isGeometry()).isTrue();
        assertThat(geography.isGeography()).isTrue();
        assertThat(geography.type())
                .isEqualTo(new IcebergType.GeographyType(
                        ParquetCrs.reference("OGC:CRS84"), Optional.of(EdgeInterpolationAlgorithm.KARNEY)));
    }

    @Test
    void rejectsAnUnsupportedTokenAtConstruction() {
        assertThatThrownBy(() -> new IcebergField(1, "v", "variant", false))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("variant");
    }
}
