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
package io.tileverse.parquetry.schema.geo.projjson;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Coverage for the {@link CoordinateReferenceSystems} facade and the bundled EPSG / OGC resource set. */
class CoordinateReferenceSystemsTest {

    @Test
    void forEpsg4326ReturnsBundledGeographicCrs() {
        Optional<CoordinateReferenceSystem> crs = CoordinateReferenceSystems.forEpsg(4326);
        assertThat(crs).hasValueSatisfying(value -> {
            assertThat(value).isInstanceOf(GeographicCRS.class);
            assertThat(value.id()).hasValueSatisfying(id -> {
                assertThat(id.authority()).isEqualTo("EPSG");
                assertThat(id.epsgCode()).hasValue(4326);
            });
        });
    }

    @Test
    void forEpsgUnknownReturnsEmpty() {
        // 99999999 is well outside any practical EPSG range; this resource cannot exist in the bundle.
        assertThat(CoordinateReferenceSystems.forEpsg(99999999)).isEmpty();
    }

    @Test
    void forEpsgNegativeCodeReturnsEmpty() {
        assertThat(CoordinateReferenceSystems.forEpsg(-1)).isEmpty();
    }

    @Test
    void forEpsgMemoisesPositiveResultsByInstance() {
        // The cache returns the same Optional (and therefore the same wrapped CRS instance) on repeat lookups.
        Optional<CoordinateReferenceSystem> first = CoordinateReferenceSystems.forEpsg(4326);
        Optional<CoordinateReferenceSystem> second = CoordinateReferenceSystems.forEpsg(4326);
        assertThat(first).isPresent();
        assertThat(second.orElseThrow()).isSameAs(first.orElseThrow());
    }

    @Test
    void forEpsgReturnsEmptyForMissingCode() {
        // Two lookups for the same missing code return Optional.empty. The cache absorbs repeat I/O attempts; the
        // observable contract is the empty Optional itself.
        Optional<CoordinateReferenceSystem> first = CoordinateReferenceSystems.forEpsg(98765432);
        Optional<CoordinateReferenceSystem> second = CoordinateReferenceSystems.forEpsg(98765432);
        assertThat(first).isEmpty();
        assertThat(second).isEmpty();
    }

    @Test
    void ogcCrs84IsAGeographicCrs() {
        CoordinateReferenceSystem crs = CoordinateReferenceSystems.ogcCrs84();
        assertThat(crs).isInstanceOf(GeographicCRS.class);
        assertThat(crs.id()).hasValueSatisfying(id -> {
            assertThat(id.authority()).isEqualTo("OGC");
            assertThat(id.code()).isEqualTo("CRS84");
        });
    }

    @Test
    void ogcCrs84IsStable() {
        // Two calls return the same instance; the constant is loaded once on first use.
        assertThat(CoordinateReferenceSystems.ogcCrs84()).isSameAs(CoordinateReferenceSystems.ogcCrs84());
    }

    @Test
    void everyBundledEpsgCodeDeserializes() {
        IntStream.concat(IntStream.of(4326, 4269, 4258, 3857, 3395), IntStream.rangeClosed(32601, 32660))
                .forEach(CoordinateReferenceSystemsTest::assertBundledEpsg);
        IntStream.rangeClosed(32701, 32760).forEach(CoordinateReferenceSystemsTest::assertBundledEpsg);
    }

    private static void assertBundledEpsg(int code) {
        Optional<CoordinateReferenceSystem> crs = CoordinateReferenceSystems.forEpsg(code);
        assertThat(crs).as("EPSG:%d should be bundled", code).isPresent();
        assertThat(crs.orElseThrow().id())
                .as("EPSG:%d should carry its EPSG identifier", code)
                .hasValueSatisfying(id -> assertThat(id.epsgCode()).hasValue(code));
    }

    @Test
    void roundTripFromAndToProjjson() {
        CoordinateReferenceSystem original = sampleGeographicCrs();
        String json = CoordinateReferenceSystems.toProjjson(original);
        CoordinateReferenceSystem parsed = CoordinateReferenceSystems.fromProjjson(json);
        assertThat(parsed).isEqualTo(original);
    }

    // Reading a bundled CRS and writing it back must change nothing. PROJ rejects a document that drops the
    // datum_ensemble of OGC:CRS84, EPSG:4326 and the CRSs derived from them.
    @ParameterizedTest
    @MethodSource("bundledResources")
    void bundledCrsRoundTripsVerbatim(String resource) {
        String source = readResource(resource);
        String written = CoordinateReferenceSystems.toProjjson(CoordinateReferenceSystems.fromProjjson(source));
        assertThat(parseTree(written)).as(resource).isEqualTo(parseTree(source));
    }

    static Stream<String> bundledResources() {
        Stream<String> ogc = Stream.of("ogc/CRS84.json");
        Stream<String> epsg = bundledEpsgCodes().map(code -> "epsg/EPSG_" + code + ".json");
        return Stream.concat(ogc, epsg);
    }

    private static Stream<Integer> bundledEpsgCodes() {
        Path directory = resourceDirectory("epsg");
        try (Stream<Path> files = Files.list(directory)) {
            return files
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .filter(name -> name.startsWith("EPSG_") && name.endsWith(".json"))
                    .map(name -> name.substring("EPSG_".length(), name.length() - ".json".length()))
                    .map(Integer::valueOf)
                    .sorted()
                    .toList()
                    .stream();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Path resourceDirectory(String name) {
        URL url = CoordinateReferenceSystems.class.getResource(name);
        assertThat(url).as("bundled resource directory %s", name).isNotNull();
        try {
            return Path.of(url.toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String readResource(String resource) {
        try (InputStream in = CoordinateReferenceSystems.class.getResourceAsStream(resource)) {
            assertThat(in).as(resource).isNotNull();
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static JsonNode parseTree(String json) {
        return JsonMapper.builder().build().readTree(json);
    }

    @Test
    void fromProjjsonHandlesUnknownType() {
        String json = "{\"type\":\"FuturisticCRS\",\"name\":\"Mystery\"}";
        CoordinateReferenceSystem parsed = CoordinateReferenceSystems.fromProjjson(json);
        assertThat(parsed).isInstanceOfSatisfying(CoordinateReferenceSystem.Unknown.class, u -> {
            assertThat(u.rawType()).isEqualTo("FuturisticCRS");
        });
    }

    private static GeographicCRS sampleGeographicCrs() {
        Ellipsoid ellipsoid = new Ellipsoid(
                Optional.of("WGS 84"),
                6378137.0,
                OptionalDouble.empty(),
                OptionalDouble.of(298.257223563),
                Optional.of(new Identifier("EPSG", "7030", Optional.empty(), Optional.empty())));
        GeodeticReferenceFrame datum = new GeodeticReferenceFrame(
                Optional.of("World Geodetic System 1984"),
                ellipsoid,
                Optional.empty(),
                Optional.of(new Identifier("EPSG", "6326", Optional.empty(), Optional.empty())));
        return new GeographicCRS(
                Optional.of("WGS 84"),
                Optional.of(new Identifier("EPSG", "4326", Optional.empty(), Optional.empty())),
                Optional.of(datum),
                Optional.of(new CoordinateSystem(Optional.of("ellipsoidal"), List.of())));
    }
}
