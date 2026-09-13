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
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.tileverse.parquetry.format.EdgeInterpolationAlgorithm;
import io.tileverse.parquetry.format.LogicalType.TimeUnit;
import io.tileverse.parquetry.schema.geo.ParquetCrs;

class IcebergTypeTest {

    private static final Optional<ParquetCrs> CRS84 = ParquetCrs.reference("OGC:CRS84");

    @ParameterizedTest
    @MethodSource("tokens")
    void parsesToken(String token, IcebergType expected) {
        assertThat(IcebergType.parse(token)).isEqualTo(expected);
    }

    static Stream<Arguments> tokens() {
        Optional<ParquetCrs> epsg3857 = Optional.of(new ParquetCrs.AuthorityCode("EPSG", "3857"));
        return Stream.of(
                arguments("boolean", new IcebergType.BoolType()),
                arguments("int", new IcebergType.IntType()),
                arguments("long", new IcebergType.LongType()),
                arguments("float", new IcebergType.FloatType()),
                arguments("double", new IcebergType.DoubleType()),
                arguments("date", new IcebergType.DateType()),
                arguments("time", new IcebergType.TimeType()),
                arguments("timestamp", new IcebergType.TimestampType(false, TimeUnit.MICROS)),
                arguments("timestamptz", new IcebergType.TimestampType(true, TimeUnit.MICROS)),
                arguments("timestamp_ns", new IcebergType.TimestampType(false, TimeUnit.NANOS)),
                arguments("timestamptz_ns", new IcebergType.TimestampType(true, TimeUnit.NANOS)),
                arguments("string", new IcebergType.StringType()),
                arguments("uuid", new IcebergType.UuidType()),
                arguments("binary", new IcebergType.BinaryType()),
                arguments("unknown", new IcebergType.UnknownType()),
                arguments("decimal(9, 2)", new IcebergType.DecimalType(9, 2)),
                arguments("decimal(38,0)", new IcebergType.DecimalType(38, 0)),
                arguments("  decimal( 20 , 3 ) ", new IcebergType.DecimalType(20, 3)),
                arguments("fixed[16]", new IcebergType.FixedType(16)),
                arguments("fixed[ 4 ]", new IcebergType.FixedType(4)),
                arguments("geometry", new IcebergType.GeometryType(CRS84)),
                arguments("geography", new IcebergType.GeographyType(CRS84, Optional.empty())),
                arguments("geometry(EPSG:3857)", new IcebergType.GeometryType(epsg3857)),
                arguments("geometry(srid:0)", new IcebergType.GeometryType(Optional.of(new ParquetCrs.Srid(0)))),
                arguments(
                        "geography(OGC:CRS84, karney)",
                        new IcebergType.GeographyType(CRS84, Optional.of(EdgeInterpolationAlgorithm.KARNEY))),
                arguments("geometry(nonsense)", new IcebergType.GeometryType(Optional.empty())),
                arguments(
                        "geography(EPSG:4326, bogus)",
                        new IcebergType.GeographyType(
                                Optional.of(new ParquetCrs.AuthorityCode("EPSG", "4326")), Optional.empty())),
                arguments("geometry( EPSG:3857 )", new IcebergType.GeometryType(epsg3857)));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "",
                "variant",
                "struct",
                "timestamp_ms",
                "decimal",
                "decimal(9)",
                "decimal(0, 0)",
                "decimal(39, 0)",
                "decimal(5, 6)",
                "decimal(99999999999, 0)",
                "fixed",
                "fixed[0]",
                "fixed(16)",
                "geometry(",
                "geometry()",
                "geography()",
                "geometry(,)",
                "geography(,)",
                "geometry(EPSG:3857, karney)",
                "geography( , karney)",
                "geometry({\"type\":\"ProjectedCRS\"})",
                "geography({\"id\":1}, karney)",
                "geometryx"
            })
    void rejectsMalformedOrUnsupportedToken(String token) {
        assertThatThrownBy(() -> IcebergType.parse(token))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining(token.isEmpty() ? "unsupported" : token.strip());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "boolean",
                "int",
                "long",
                "float",
                "double",
                "date",
                "time",
                "timestamp",
                "timestamptz",
                "timestamp_ns",
                "timestamptz_ns",
                "string",
                "uuid",
                "binary",
                "unknown",
                "decimal(9, 2)",
                "fixed[16]",
                "geometry",
                "geography",
                "geometry(EPSG:3857)",
                "geometry(srid:0)",
                "geography(OGC:CRS84, karney)",
                "geography(EPSG:4326, andoyer)"
            })
    void tokenRoundTrips(String token) {
        IcebergType parsed = IcebergType.parse(token);
        assertThat(IcebergType.parse(parsed.token())).isEqualTo(parsed);
    }

    @Test
    void rendersCanonicalTokens() {
        assertThat(IcebergType.parse("decimal(9,2)").token()).isEqualTo("decimal(9, 2)");
        assertThat(IcebergType.parse("fixed[ 4 ]").token()).isEqualTo("fixed[4]");
        assertThat(IcebergType.parse("timestamptz_ns").token()).isEqualTo("timestamptz_ns");
        assertThat(IcebergType.parse("geometry(OGC:CRS84)").token()).isEqualTo("geometry");
        assertThat(IcebergType.parse("geography(EPSG:4326, karney)").token()).isEqualTo("geography(EPSG:4326, karney)");
        assertThat(IcebergType.parse("geography(OGC:CRS84, karney)").token()).isEqualTo("geography(OGC:CRS84, karney)");
    }

    @Test
    void recordsRejectOutOfRangeParameters() {
        assertThatThrownBy(() -> new IcebergType.DecimalType(39, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IcebergType.DecimalType(4, 5)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IcebergType.FixedType(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new IcebergType.TimestampType(true, TimeUnit.MILLIS))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void everyMemberRendersATokenThatParsesBack() {
        List<IcebergType> members = List.of(
                new IcebergType.BoolType(),
                new IcebergType.IntType(),
                new IcebergType.LongType(),
                new IcebergType.FloatType(),
                new IcebergType.DoubleType(),
                new IcebergType.DateType(),
                new IcebergType.StringType(),
                new IcebergType.BinaryType(),
                new IcebergType.UuidType(),
                new IcebergType.TimestampType(false, TimeUnit.MICROS),
                new IcebergType.TimeType(),
                new IcebergType.DecimalType(1, 0),
                new IcebergType.FixedType(1),
                new IcebergType.GeometryType(CRS84),
                new IcebergType.GeographyType(CRS84, Optional.empty()),
                new IcebergType.UnknownType());
        for (IcebergType member : members) {
            assertThat(IcebergType.parse(member.token())).as(member.token()).isEqualTo(member);
        }
    }

    @Test
    void aGeoTypeWithoutACrsRendersAsTheBareKind() {
        assertThat(new IcebergType.GeometryType(Optional.empty()).token()).isEqualTo("geometry");
        assertThat(new IcebergType.GeographyType(Optional.empty(), Optional.of(EdgeInterpolationAlgorithm.KARNEY))
                        .token())
                .isEqualTo("geography(OGC:CRS84, karney)");
    }
}
