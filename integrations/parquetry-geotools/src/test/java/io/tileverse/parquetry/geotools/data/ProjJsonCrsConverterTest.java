/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import java.util.Optional;

import org.geotools.referencing.CRS;
import org.junit.jupiter.api.Test;

class ProjJsonCrsConverterTest {

    @Test
    void emptyCrsDefaultsToCrs84LonLat() {
        org.geotools.api.referencing.crs.CoordinateReferenceSystem crs =
                ProjJsonCrsConverter.toGeoTools(Optional.empty());
        assertThat(crs).isNotNull();
        assertThat(CRS.getAxisOrder(crs)).isEqualTo(CRS.AxisOrder.EAST_NORTH); // lon/lat
    }

    @Test
    void epsgIdentifierDecodesLongitudeFirst() throws Exception {
        io.tileverse.parquetry.schema.geo.projjson.Identifier epsg4326 =
                new io.tileverse.parquetry.schema.geo.projjson.Identifier(
                        "EPSG", "4326", Optional.empty(), Optional.empty());
        io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem projjson =
                new io.tileverse.parquetry.schema.geo.projjson.GeographicCRS(
                        Optional.of("WGS 84"), Optional.of(epsg4326), Optional.empty(), Optional.empty());
        org.geotools.api.referencing.crs.CoordinateReferenceSystem crs =
                ProjJsonCrsConverter.toGeoTools(Optional.of(projjson));
        assertThat(CRS.lookupEpsgCode(crs, false)).isEqualTo(4326);
        assertThat(CRS.getAxisOrder(crs)).isEqualTo(CRS.AxisOrder.EAST_NORTH);
    }

    @Test
    void authorityCodeReferenceDecodesThroughTheRegistry() throws Exception {
        org.geotools.api.referencing.crs.CoordinateReferenceSystem crs = ProjJsonCrsConverter.toGeoTools(
                new io.tileverse.parquetry.schema.geo.ParquetCrs.AuthorityCode("EPSG", "3857"));
        assertThat(CRS.lookupEpsgCode(crs, false)).isEqualTo(3857);
        assertThat(CRS.getAxisOrder(crs)).isEqualTo(CRS.AxisOrder.EAST_NORTH);
    }

    @Test
    void ogcCrs84ReferenceResolvesToWgs84LonLat() {
        org.geotools.api.referencing.crs.CoordinateReferenceSystem crs = ProjJsonCrsConverter.toGeoTools(
                new io.tileverse.parquetry.schema.geo.ParquetCrs.AuthorityCode("OGC", "CRS84"));
        assertThat(crs).isNotNull();
        assertThat(CRS.getAxisOrder(crs)).isEqualTo(CRS.AxisOrder.EAST_NORTH);
    }

    @Test
    void sridReferenceDecodesAsEpsgCode() throws Exception {
        org.geotools.api.referencing.crs.CoordinateReferenceSystem crs =
                ProjJsonCrsConverter.toGeoTools(new io.tileverse.parquetry.schema.geo.ParquetCrs.Srid(3857));
        assertThat(CRS.lookupEpsgCode(crs, false)).isEqualTo(3857);
    }

    @Test
    void sridZeroReferenceIsUndefinedAndDefaultsToWgs84() {
        org.geotools.api.referencing.crs.CoordinateReferenceSystem crs =
                ProjJsonCrsConverter.toGeoTools(new io.tileverse.parquetry.schema.geo.ParquetCrs.Srid(0));
        assertThat(crs).isNotNull();
        assertThat(CRS.getAxisOrder(crs)).isEqualTo(CRS.AxisOrder.EAST_NORTH);
    }

    @Test
    void unresolvableAuthorityCodeFallsBackToWgs84() {
        org.geotools.api.referencing.crs.CoordinateReferenceSystem crs = ProjJsonCrsConverter.toGeoTools(
                new io.tileverse.parquetry.schema.geo.ParquetCrs.AuthorityCode("EPSG", "999999"));
        assertThat(crs).isNotNull();
        assertThat(CRS.getAxisOrder(crs)).isEqualTo(CRS.AxisOrder.EAST_NORTH);
    }
}
