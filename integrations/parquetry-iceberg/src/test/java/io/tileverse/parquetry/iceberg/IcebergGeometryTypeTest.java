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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.tileverse.parquetry.format.EdgeInterpolationAlgorithm;
import io.tileverse.parquetry.schema.geo.ParquetCrs;

class IcebergGeometryTypeTest {

    @ParameterizedTest
    @MethodSource("tokens")
    void parsesToken(
            String token, String baseKind, Optional<ParquetCrs> crs, Optional<EdgeInterpolationAlgorithm> algorithm) {
        IcebergGeometryType parsed = IcebergGeometryType.parse(token);
        assertThat(parsed.baseKind()).isEqualTo(baseKind);
        assertThat(parsed.crs()).isEqualTo(crs);
        assertThat(parsed.algorithm()).isEqualTo(algorithm);
    }

    static Stream<Arguments> tokens() {
        Optional<ParquetCrs> crs84 = ParquetCrs.reference("OGC:CRS84");
        Optional<ParquetCrs> epsg3857 = Optional.of(new ParquetCrs.AuthorityCode("EPSG", "3857"));
        return Stream.of(
                arguments("geometry", "geometry", crs84, Optional.empty()),
                arguments("geography", "geography", crs84, Optional.empty()),
                arguments("geometry(EPSG:3857)", "geometry", epsg3857, Optional.empty()),
                arguments("geometry(srid:0)", "geometry", Optional.of(new ParquetCrs.Srid(0)), Optional.empty()),
                arguments(
                        "geography(OGC:CRS84, karney)",
                        "geography",
                        crs84,
                        Optional.of(EdgeInterpolationAlgorithm.KARNEY)),
                arguments("geometry(nonsense)", "geometry", Optional.empty(), Optional.empty()),
                arguments(
                        "geography(EPSG:4326, bogus)",
                        "geography",
                        Optional.of(new ParquetCrs.AuthorityCode("EPSG", "4326")),
                        Optional.empty()),
                arguments("geometry( EPSG:3857 )", "geometry", epsg3857, Optional.empty()));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "geometry(",
                "geometry()",
                "geography()",
                "geometry(,)",
                "geography(,)",
                "geometry(EPSG:3857, karney)",
                "geography( , karney)"
            })
    void rejectsMalformedToken(String token) {
        assertThatThrownBy(() -> IcebergGeometryType.parse(token)).isInstanceOf(IcebergFormatException.class);
    }

    @Test
    void rejectsNonGeoToken() {
        assertThatThrownBy(() -> IcebergGeometryType.parse("string")).isInstanceOf(IcebergFormatException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"geometry({\"type\":\"ProjectedCRS\"})", "geography({\"id\":1}, karney)"})
    void rejectsInlineProjjsonCrs(String token) {
        assertThatThrownBy(() -> IcebergGeometryType.parse(token)).isInstanceOf(IcebergFormatException.class);
    }

    @Test
    void recognizesGeoTokens() {
        assertThat(IcebergGeometryType.isGeometryToken("geometry")).isTrue();
        assertThat(IcebergGeometryType.isGeometryToken("geometry(EPSG:3857)")).isTrue();
        assertThat(IcebergGeometryType.isGeometryToken("geography(OGC:CRS84, karney)"))
                .isTrue();
        assertThat(IcebergGeometryType.isGeometryToken("string")).isFalse();
        assertThat(IcebergGeometryType.isGeometryToken("binary")).isFalse();
    }
}
