/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.schema.geo.projjson;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.InputStream;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.json.JsonMapper;

/**
 * Coverage for the PROJJSON typed ADT. Parses the geotools test corpus (geographic, projected, compound, bound,
 * transformation) plus parquetry-specific cases (Unknown discriminator forward-compat, missing-type fallback,
 * Identifier int/string coercion, EPSG code derivation).
 */
class ProjJsonModuleTest {

    private static final JsonMapper MAPPER =
            JsonMapper.builder().addModule(new ProjJsonModule()).build();

    @Test
    void parsesGeographicCrs() throws IOException {
        CoordinateReferenceSystem crs = read("geographic_crs.json");
        assertThat(crs).isInstanceOfSatisfying(GeographicCRS.class, gcrs -> {
            assertThat(gcrs.name()).contains("NAD83(2011)");
            assertThat(gcrs.id()).hasValueSatisfying(id -> {
                assertThat(id.authority()).isEqualTo("EPSG");
                assertThat(id.code()).isEqualTo("6318");
                assertThat(id.epsgCode()).hasValue(6318);
            });
            assertThat(gcrs.datum()).isPresent();
            assertThat(gcrs.datum().orElseThrow().ellipsoid().name()).contains("GRS 1980");
            assertThat(gcrs.coordinateSystem()).isPresent();
            assertThat(gcrs.coordinateSystem().orElseThrow().axes()).hasSize(2);
        });
    }

    @Test
    void parsesProjectedCrs() throws IOException {
        CoordinateReferenceSystem crs = read("projected_crs.json");
        ProjectedCRS pcrs = (ProjectedCRS) crs;
        assertThat(pcrs.name()).contains("WGS 84 / UTM zone 31N");
        assertThat(pcrs.id().orElseThrow().epsgCode()).hasValue(32631);
        assertThat(pcrs.baseCrs())
                .as("base CRS is polymorphic and dispatches back through CoordinateReferenceSystemDeserializer")
                .hasValueSatisfying(base -> assertThat(base).isInstanceOf(GeographicCRS.class));
        Conversion conv = pcrs.conversion().orElseThrow();
        assertThat(conv.method().name()).contains("Transverse Mercator");
        assertThat(conv.parameters()).hasSize(5);
        assertThat(conv.method().id().orElseThrow().epsgCode())
                .as("EPSG operation method id for Transverse Mercator")
                .hasValue(9807);
    }

    @Test
    void parsesCompoundCrs() throws IOException {
        CoordinateReferenceSystem crs = read("compound_crs.json");
        CompoundCRS ccrs = (CompoundCRS) crs;
        assertThat(ccrs.components()).hasSizeGreaterThanOrEqualTo(2);
        assertThat(ccrs.components().get(0))
                .as("first component is the horizontal CRS")
                .isInstanceOfAny(GeographicCRS.class, ProjectedCRS.class, BoundCRS.class);
    }

    @Test
    void parsesBoundCrs() throws IOException {
        CoordinateReferenceSystem crs = read("bound_crs.json");
        BoundCRS bcrs = (BoundCRS) crs;
        assertThat(bcrs.sourceCrs()).isPresent();
        assertThat(bcrs.targetCrs()).isPresent();
        assertThat(bcrs.transformation()).isPresent();
    }

    @Test
    void parsesTransformation() throws IOException {
        CoordinateReferenceSystem crs = read("transformation.json");
        assertThat(crs).isInstanceOf(Transformation.class);
        Transformation t = (Transformation) crs;
        assertThat(t.sourceCrs()).isPresent();
        assertThat(t.targetCrs()).isPresent();
    }

    @Test
    void unknownTypeFallsBackToUnknownRecord() {
        String json = """
                {"type": "TemporalCRS", "name": "Calendar time", "id": {"authority": "PROJ", "code": "Calendar"}}
                """;
        CoordinateReferenceSystem crs = MAPPER.readValue(json, CoordinateReferenceSystem.class);
        assertThat(crs).isInstanceOfSatisfying(CoordinateReferenceSystem.Unknown.class, u -> {
            assertThat(u.rawType()).isEqualTo("TemporalCRS");
            assertThat(u.raw().get("name").asString()).isEqualTo("Calendar time");
        });
    }

    @Test
    void missingTypeFallsBackToUnknownRecord() {
        String json = "{\"name\": \"mystery CRS\"}";
        CoordinateReferenceSystem crs = MAPPER.readValue(json, CoordinateReferenceSystem.class);
        assertThat(crs).isInstanceOfSatisfying(CoordinateReferenceSystem.Unknown.class, u -> {
            assertThat(u.rawType()).isEmpty();
            assertThat(u.raw().get("name").asString()).isEqualTo("mystery CRS");
        });
    }

    @Test
    void identifierAcceptsIntCode() {
        String json = "{\"authority\": \"EPSG\", \"code\": 4326}";
        Identifier id = MAPPER.readValue(json, Identifier.class);
        assertThat(id.code()).isEqualTo("4326");
        assertThat(id.epsgCode()).hasValue(4326);
    }

    @Test
    void identifierAcceptsStringCode() {
        String json = "{\"authority\": \"OGC\", \"code\": \"CRS84\"}";
        Identifier id = MAPPER.readValue(json, Identifier.class);
        assertThat(id.code()).isEqualTo("CRS84");
        assertThat(id.epsgCode()).isEmpty();
    }

    @Test
    void nonEpsgAuthorityYieldsEmptyEpsgCode() {
        Identifier id = new Identifier("OGC", "4326", java.util.Optional.empty(), java.util.Optional.empty());
        assertThat(id.epsgCode()).isEmpty();
    }

    @Test
    void unparsableCodeYieldsEmptyEpsgCode() {
        Identifier id = new Identifier("EPSG", "v9", java.util.Optional.empty(), java.util.Optional.empty());
        assertThat(id.epsgCode()).isEmpty();
    }

    @Test
    void negativeCodeYieldsEmptyEpsgCode() {
        Identifier id = new Identifier("EPSG", "-1", java.util.Optional.empty(), java.util.Optional.empty());
        assertThat(id.epsgCode()).isEmpty();
    }

    @Test
    void moduleIsNotAutoRegistered() {
        JsonMapper plain = JsonMapper.builder().build();
        String json = "{\"type\": \"GeographicCRS\", \"name\": \"WGS 84\"}";
        assertThatThrownBy(() -> plain.readValue(json, CoordinateReferenceSystem.class))
                .as("a fresh mapper without ProjJsonModule must NOT find a deserializer via SPI auto-registration")
                .isInstanceOf(Exception.class);
    }

    @Test
    void identifierMissingCodeProducesNullCode() {
        Identifier id = MAPPER.readValue("{\"authority\": \"EPSG\"}", Identifier.class);
        assertThat(id.authority()).isEqualTo("EPSG");
        assertThat(id.code()).isNull();
        assertThat(id.epsgCode()).isEmpty();
    }

    private static CoordinateReferenceSystem read(String resource) throws IOException {
        try (InputStream in = ProjJsonModuleTest.class.getResourceAsStream(resource)) {
            assertThat(in).as("classpath:" + resource).isNotNull();
            return MAPPER.readValue(in, CoordinateReferenceSystem.class);
        }
    }
}
