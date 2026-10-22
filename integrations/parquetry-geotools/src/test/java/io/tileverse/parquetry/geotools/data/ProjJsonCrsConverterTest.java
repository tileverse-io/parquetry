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

import java.util.Optional;

import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.referencing.CRS;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.tileverse.parquetry.schema.geo.ParquetCrs;
import io.tileverse.parquetry.schema.geo.geoparquet.ColumnCrs;
import io.tileverse.parquetry.schema.geo.projjson.GeographicCRS;
import io.tileverse.parquetry.schema.geo.projjson.Identifier;

class ProjJsonCrsConverterTest {

    @Test
    void absentCrsDefaultsToCrs84() {
        Optional<CoordinateReferenceSystem> crs = ProjJsonCrsConverter.toGeoTools(ColumnCrs.DEFAULTED);
        assertThat(crs).isPresent();
        assertThat(CRS.getAxisOrder(crs.orElseThrow())).isEqualTo(CRS.AxisOrder.EAST_NORTH);
    }

    // "crs": null means the CRS is unknown, which is not the same as absent. Standing in with WGS 84 would state
    // degrees for data that may be in metres.
    @Test
    void unknownCrsResolvesToNothing() {
        assertThat(ProjJsonCrsConverter.toGeoTools(ColumnCrs.UNKNOWN)).isEmpty();
    }

    @Test
    void epsgIdentifierDecodesLonFirst() throws Exception {
        ColumnCrs declared =
                ColumnCrs.of(geographicCrs(new Identifier("EPSG", "4326", Optional.empty(), Optional.empty())));
        CoordinateReferenceSystem crs =
                ProjJsonCrsConverter.toGeoTools(declared).orElseThrow();
        assertThat(CRS.lookupEpsgCode(crs, false)).isEqualTo(4326);
        assertThat(CRS.getAxisOrder(crs)).isEqualTo(CRS.AxisOrder.EAST_NORTH);
    }

    // The GeoTools registry in use here holds EPSG only, hence an ESRI code resolves to nothing rather than to a
    // stand-in. ESRI:102003 is in metres; reading it as WGS 84 would misplace every coordinate.
    @Test
    void nonEpsgInlineCrsResolvesToNothing() {
        ColumnCrs declared =
                ColumnCrs.of(geographicCrs(new Identifier("ESRI", "102003", Optional.empty(), Optional.empty())));
        assertThat(ProjJsonCrsConverter.toGeoTools(declared)).isEmpty();
    }

    @Test
    void inlineCrsWithoutIdResolvesToNothing() {
        GeographicCRS noIdentifier =
                new GeographicCRS(Optional.of("Some Local CRS"), Optional.empty(), Optional.empty(), Optional.empty());
        assertThat(ProjJsonCrsConverter.toGeoTools(ColumnCrs.of(noIdentifier))).isEmpty();
    }

    @Test
    void authorityCodeDecodesThroughRegistry() throws Exception {
        CoordinateReferenceSystem crs = ProjJsonCrsConverter.toGeoTools(new ParquetCrs.AuthorityCode("EPSG", "3857"))
                .orElseThrow();
        assertThat(CRS.lookupEpsgCode(crs, false)).isEqualTo(3857);
        assertThat(CRS.getAxisOrder(crs)).isEqualTo(CRS.AxisOrder.EAST_NORTH);
    }

    @Test
    void ogcCrs84ResolvesToWgs84LonLat() {
        Optional<CoordinateReferenceSystem> crs =
                ProjJsonCrsConverter.toGeoTools(new ParquetCrs.AuthorityCode("OGC", "CRS84"));
        assertThat(crs).isPresent();
        assertThat(CRS.getAxisOrder(crs.orElseThrow())).isEqualTo(CRS.AxisOrder.EAST_NORTH);
    }

    @Test
    void sridDecodesAsEpsgCode() throws Exception {
        CoordinateReferenceSystem crs =
                ProjJsonCrsConverter.toGeoTools(new ParquetCrs.Srid(3857)).orElseThrow();
        assertThat(CRS.lookupEpsgCode(crs, false)).isEqualTo(3857);
    }

    @Test
    void sridZeroResolvesToNothing() {
        assertThat(ProjJsonCrsConverter.toGeoTools(new ParquetCrs.Srid(0))).isEmpty();
    }

    @Test
    void unresolvableCodeResolvesToNothing() {
        assertThat(ProjJsonCrsConverter.toGeoTools(new ParquetCrs.AuthorityCode("EPSG", "999999")))
                .isEmpty();
    }

    @Test
    void projjsonKeyResolvesToNothing() {
        assertThat(ProjJsonCrsConverter.toGeoTools(new ParquetCrs.ProjjsonKey("some-key")))
                .isEmpty();
    }

    /**
     * A GeoTools factory can hand back a cached CRS for a code it has already decoded, and the lon/lat and lat/lon
     * forms of one code share an identifier. One process asks for both: this converter always asks lon-first, and
     * GeoServer asks in authority order elsewhere, for instance for a WFS 2.0 URN. Decoding a code both ways, in either
     * sequence, must give the order asked for each time.
     */
    @ParameterizedTest
    @ValueSource(strings = {"EPSG:4326", "EPSG:4258", "EPSG:4269"})
    void axisOrderFollowsEachRequest(String code) throws Exception {
        assertDecodesBothWays(code);
        assertDecodesBothWays(code);
    }

    private static void assertDecodesBothWays(String code) throws Exception {
        assertThat(CRS.getAxisOrder(CRS.decode(code, true)))
                .as("%s asked lon-first", code)
                .isEqualTo(CRS.AxisOrder.EAST_NORTH);
        assertThat(CRS.getAxisOrder(CRS.decode(code, false)))
                .as("%s asked in authority order", code)
                .isEqualTo(CRS.AxisOrder.NORTH_EAST);
    }

    private static GeographicCRS geographicCrs(Identifier id) {
        return new GeographicCRS(Optional.of("a CRS"), Optional.of(id), Optional.empty(), Optional.empty());
    }
}
