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

import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Round-trip coverage for {@link CoordinateReferenceSystemSerializer}: every concrete CRS case serialises with a
 * leading {@code "type"} discriminator and re-parses back to the same record through the polymorphic
 * {@link CoordinateReferenceSystemDeserializer}. {@link CoordinateReferenceSystem.Unknown} round-trips the captured raw
 * JSON tree verbatim.
 */
class CoordinateReferenceSystemSerializerTest {

    private static final JsonMapper MAPPER =
            JsonMapper.builder().addModule(new ProjJsonModule()).build();

    @Test
    void serializesGeographicCrsWithTypeDiscriminator() {
        CoordinateReferenceSystem crs = sampleGeographicCrs();
        String json = MAPPER.writeValueAsString(crs);

        assertThat(json).contains("\"type\":\"GeographicCRS\"", "\"name\":\"WGS 84\"");

        CoordinateReferenceSystem parsed = MAPPER.readValue(json, CoordinateReferenceSystem.class);
        assertThat(parsed).isEqualTo(crs);
    }

    @Test
    void typeDiscriminatorIsLeadingField() {
        CoordinateReferenceSystem crs = sampleGeographicCrs();
        String json = MAPPER.writeValueAsString(crs);
        assertThat(json).startsWith("{\"type\":\"GeographicCRS\"");
    }

    @Test
    void serializesGeodeticCrs() {
        GeodeticCRS crs = new GeodeticCRS(
                Optional.of("WGS 84 (geocentric)"),
                Optional.of(new Identifier("EPSG", "4978", Optional.empty(), Optional.empty())),
                Optional.of(wgs84Datum()),
                Optional.empty());

        String json = MAPPER.writeValueAsString(crs);
        assertThat(json).contains("\"type\":\"GeodeticCRS\"");

        CoordinateReferenceSystem parsed = MAPPER.readValue(json, CoordinateReferenceSystem.class);
        assertThat(parsed).isEqualTo(crs);
    }

    @Test
    void serializesProjectedCrs() {
        ProjectedCRS crs = sampleProjectedCrs();
        String json = MAPPER.writeValueAsString(crs);

        assertThat(json).contains("\"type\":\"ProjectedCRS\"", "\"base_crs\"");

        CoordinateReferenceSystem parsed = MAPPER.readValue(json, CoordinateReferenceSystem.class);
        assertThat(parsed)
                .isEqualTo(crs)
                .isInstanceOfSatisfying(
                        ProjectedCRS.class,
                        p -> assertThat(p.baseCrs())
                                .hasValueSatisfying(base -> assertThat(base).isInstanceOf(GeographicCRS.class)));
    }

    @Test
    void serializesCompoundCrs() {
        CompoundCRS crs = new CompoundCRS(
                Optional.of("WGS 84 + EGM2008 height"),
                Optional.empty(),
                List.of(sampleGeographicCrs(), sampleVerticalCrs()));

        String json = MAPPER.writeValueAsString(crs);
        assertThat(json).contains("\"type\":\"CompoundCRS\"");

        CoordinateReferenceSystem parsed = MAPPER.readValue(json, CoordinateReferenceSystem.class);
        assertThat(parsed).isEqualTo(crs);
    }

    @Test
    void serializesBoundCrs() {
        BoundCRS crs = new BoundCRS(
                Optional.of("test bound"),
                Optional.empty(),
                Optional.of(sampleProjectedCrs()),
                Optional.of(sampleGeographicCrs()),
                Optional.of(sampleTransformation()));

        String json = MAPPER.writeValueAsString(crs);
        assertThat(json).contains("\"type\":\"BoundCRS\"", "\"source_crs\"", "\"target_crs\"");

        CoordinateReferenceSystem parsed = MAPPER.readValue(json, CoordinateReferenceSystem.class);
        assertThat(parsed).isEqualTo(crs);
    }

    @Test
    void serializesVerticalCrs() {
        VerticalCRS crs = sampleVerticalCrs();
        String json = MAPPER.writeValueAsString(crs);

        assertThat(json).contains("\"type\":\"VerticalCRS\"");

        CoordinateReferenceSystem parsed = MAPPER.readValue(json, CoordinateReferenceSystem.class);
        assertThat(parsed).isEqualTo(crs);
    }

    @Test
    void serializesTransformation() {
        Transformation t = sampleTransformation();
        String json = MAPPER.writeValueAsString(t);

        assertThat(json).contains("\"type\":\"Transformation\"");

        CoordinateReferenceSystem parsed = MAPPER.readValue(json, CoordinateReferenceSystem.class);
        assertThat(parsed).isEqualTo(t);
    }

    @Test
    void unknownReEmitsRawJsonNodeVerbatim() {
        String original = "{\"type\":\"FuturisticCRS\",\"flux\":42,\"nested\":{\"a\":[1,2,3]}}";
        CoordinateReferenceSystem.Unknown u =
                (CoordinateReferenceSystem.Unknown) MAPPER.readValue(original, CoordinateReferenceSystem.class);

        String reEmitted = MAPPER.writeValueAsString(u);
        JsonNode parsedOriginal = MAPPER.readTree(original);
        JsonNode parsedReEmitted = MAPPER.readTree(reEmitted);
        assertThat(parsedReEmitted)
                .as("Unknown round-trips the captured raw JSON tree structurally identically")
                .isEqualTo(parsedOriginal);
    }

    @Test
    void unknownRoundTripsBackToUnknownRecord() {
        String original = "{\"type\":\"FuturisticCRS\",\"flux\":42}";
        CoordinateReferenceSystem.Unknown u =
                (CoordinateReferenceSystem.Unknown) MAPPER.readValue(original, CoordinateReferenceSystem.class);

        String reEmitted = MAPPER.writeValueAsString(u);
        CoordinateReferenceSystem reParsed = MAPPER.readValue(reEmitted, CoordinateReferenceSystem.class);
        assertThat(reParsed).isInstanceOfSatisfying(CoordinateReferenceSystem.Unknown.class, again -> {
            assertThat(again.rawType()).isEqualTo("FuturisticCRS");
            assertThat(again.raw().get("flux").asInt()).isEqualTo(42);
        });
    }

    // -- sample builders --------------------------------------------------------------------------

    private static GeographicCRS sampleGeographicCrs() {
        return new GeographicCRS(
                Optional.of("WGS 84"),
                Optional.of(new Identifier("EPSG", "4326", Optional.empty(), Optional.empty())),
                Optional.of(wgs84Datum()),
                Optional.of(new CoordinateSystem(
                        Optional.of("ellipsoidal"),
                        List.of(
                                new Axis(
                                        Optional.of("Geodetic latitude"),
                                        Optional.of("Lat"),
                                        Optional.of("north"),
                                        Optional.of(MAPPER.readTree("\"degree\""))),
                                new Axis(
                                        Optional.of("Geodetic longitude"),
                                        Optional.of("Lon"),
                                        Optional.of("east"),
                                        Optional.of(MAPPER.readTree("\"degree\"")))))));
    }

    private static GeodeticReferenceFrame wgs84Datum() {
        Ellipsoid ellipsoid = new Ellipsoid(
                Optional.of("WGS 84"),
                6378137.0,
                OptionalDouble.empty(),
                OptionalDouble.of(298.257223563),
                Optional.of(new Identifier("EPSG", "7030", Optional.empty(), Optional.empty())));
        return new GeodeticReferenceFrame(
                Optional.of("World Geodetic System 1984"),
                ellipsoid,
                Optional.empty(),
                Optional.of(new Identifier("EPSG", "6326", Optional.empty(), Optional.empty())));
    }

    private static ProjectedCRS sampleProjectedCrs() {
        Conversion conversion = new Conversion(
                Optional.of("UTM zone 31N"),
                new Conversion.Method(
                        Optional.of("Transverse Mercator"),
                        Optional.of(new Identifier("EPSG", "9807", Optional.empty(), Optional.empty()))),
                List.of(
                        new Conversion.Parameter(
                                Optional.of("Latitude of natural origin"),
                                Optional.of(MAPPER.readTree("0")),
                                Optional.of(MAPPER.readTree("\"degree\"")),
                                Optional.of(new Identifier("EPSG", "8801", Optional.empty(), Optional.empty()))),
                        new Conversion.Parameter(
                                Optional.of("Longitude of natural origin"),
                                Optional.of(MAPPER.readTree("3")),
                                Optional.of(MAPPER.readTree("\"degree\"")),
                                Optional.of(new Identifier("EPSG", "8802", Optional.empty(), Optional.empty())))));

        CoordinateSystem cs = new CoordinateSystem(
                Optional.of("Cartesian"),
                List.of(
                        new Axis(
                                Optional.of("Easting"),
                                Optional.of("E"),
                                Optional.of("east"),
                                Optional.of(MAPPER.readTree("\"metre\""))),
                        new Axis(
                                Optional.of("Northing"),
                                Optional.of("N"),
                                Optional.of("north"),
                                Optional.of(MAPPER.readTree("\"metre\"")))));

        return new ProjectedCRS(
                Optional.of("WGS 84 / UTM zone 31N"),
                Optional.of(new Identifier("EPSG", "32631", Optional.empty(), Optional.empty())),
                Optional.of(sampleGeographicCrs()),
                Optional.of(conversion),
                Optional.of(cs));
    }

    private static VerticalCRS sampleVerticalCrs() {
        return new VerticalCRS(
                Optional.of("EGM2008 height"),
                Optional.of(new Identifier("EPSG", "3855", Optional.empty(), Optional.empty())),
                Optional.of(new CoordinateSystem(
                        Optional.of("vertical"),
                        List.of(new Axis(
                                Optional.of("Gravity-related height"),
                                Optional.of("H"),
                                Optional.of("up"),
                                Optional.of(MAPPER.readTree("\"metre\"")))))));
    }

    private static Transformation sampleTransformation() {
        return new Transformation(
                Optional.of("NAD27 to WGS 84 (NTv2)"),
                Optional.of(sampleGeographicCrs()),
                Optional.of(sampleGeographicCrs()),
                Optional.of(new Conversion.Method(
                        Optional.of("NTv2"),
                        Optional.of(new Identifier("EPSG", "9615", Optional.empty(), Optional.empty())))),
                // Every Conversion.Parameter field is populated: an empty Optional<JsonNode> on `unit` does not
                // round-trip cleanly through Jackson 3's default deserializer (absent fields deserialize to
                // Optional<NullNode> rather than Optional.empty), and asserting on that quirk is outside the scope of
                // this test - we cover the symmetric serializer here, not the JsonNode-handling edge cases of the
                // Deserializer.
                List.of(new Conversion.Parameter(
                        Optional.of("Latitude and longitude difference file"),
                        Optional.of(MAPPER.readTree("\"ntv2_0.gsb\"")),
                        Optional.of(MAPPER.readTree("\"unspecified\"")),
                        Optional.of(new Identifier("EPSG", "8656", Optional.empty(), Optional.empty())))),
                Optional.of("1.0"),
                Optional.empty());
    }
}
