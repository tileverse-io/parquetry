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
package io.tileverse.parquetry.format.codec;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.geo.ParquetCrs;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;
import io.tileverse.parquetry.schema.geo.projjson.GeographicCRS;

/**
 * Wire-level coverage for {@link LogicalTypeDeserializer#readGeometry} / {@code readGeography}: confirms the PROJJSON
 * payload on field 1 is parsed eagerly into the typed {@link CoordinateReferenceSystem} ADT, and that a malformed
 * payload degrades gracefully to {@link java.util.Optional#empty()} rather than failing the footer read.
 */
class LogicalTypeGeometryWireTest {

    private static final int TYPE_BINARY = 8;
    private static final int TYPE_STRUCT = 12;
    private static final int GEOMETRY_VARIANT = 17;
    private static final int GEOGRAPHY_VARIANT = 18;

    @Test
    void readsGeometryWithEagerlyParsedCrs() throws IOException {
        String projjson = "{\"type\":\"GeographicCRS\",\"name\":\"WGS 84\"}";
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeUnionVariantHeader(out, GEOMETRY_VARIANT);
        writeStringField(out, 1, projjson);
        out.write(0); // STOP inside Geometry struct
        out.write(0); // STOP for the LogicalType union

        LogicalType lt = read(out);
        assertThat(lt).as("case 17 should decode as Geometry").isInstanceOf(LogicalType.Geometry.class);
        LogicalType.Geometry geom = (LogicalType.Geometry) lt;
        assertThat(geom.crs())
                .as("PROJJSON on the wire should be parsed eagerly, not surfaced as a raw string")
                .isPresent();
        assertThat(geom.crs().orElseThrow())
                .as("inline PROJJSON should land as a ParquetCrs.Inline")
                .isInstanceOf(ParquetCrs.Inline.class);
        CoordinateReferenceSystem crs = ((ParquetCrs.Inline) geom.crs().orElseThrow()).projjson();
        assertThat(crs)
                .as("type:GeographicCRS should land as a typed GeographicCRS record")
                .isInstanceOf(GeographicCRS.class);
        assertThat(crs.name()).as("CRS name round-trips through eager parsing").contains("WGS 84");
    }

    @Test
    void readsGeographyWithEagerlyParsedCrs() throws IOException {
        String projjson = "{\"type\":\"GeographicCRS\",\"name\":\"OGC:CRS84\"}";
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeUnionVariantHeader(out, GEOGRAPHY_VARIANT);
        writeStringField(out, 1, projjson);
        out.write(0);
        out.write(0);

        LogicalType lt = read(out);
        assertThat(lt).as("case 18 should decode as Geography").isInstanceOf(LogicalType.Geography.class);
        LogicalType.Geography geog = (LogicalType.Geography) lt;
        assertThat(geog.crs())
                .as("Geography PROJJSON is parsed eagerly into the typed ADT")
                .isPresent();
        assertThat(geog.crs().orElseThrow())
                .as("inline PROJJSON should land as a ParquetCrs.Inline")
                .isInstanceOf(ParquetCrs.Inline.class);
        assertThat(((ParquetCrs.Inline) geog.crs().orElseThrow()).projjson()).isInstanceOf(GeographicCRS.class);
    }

    @Test
    void readsGeometryWithAuthorityCodeCrs() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeUnionVariantHeader(out, GEOMETRY_VARIANT);
        writeStringField(out, 1, "EPSG:3857");
        out.write(0);
        out.write(0);

        LogicalType.Geometry geom = (LogicalType.Geometry) read(out);
        assertThat(geom.crs().orElseThrow())
                .as("an authority:code native crs should parse to ParquetCrs.AuthorityCode")
                .isEqualTo(new ParquetCrs.AuthorityCode("EPSG", "3857"));
        assertThat(geom.crs().orElseThrow().epsgCode())
                .as("EPSG:3857 exposes its EPSG code without a registry lookup")
                .hasValue(3857);
    }

    @Test
    void readsGeometryWithSridZeroAsUndefinedCrs() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeUnionVariantHeader(out, GEOMETRY_VARIANT);
        writeStringField(out, 1, "srid:0");
        out.write(0);
        out.write(0);

        LogicalType.Geometry geom = (LogicalType.Geometry) read(out);
        assertThat(geom.crs().orElseThrow())
                .as("srid:0 should parse to ParquetCrs.Srid(0) (an undefined CRS, present but with no EPSG code)")
                .isEqualTo(new ParquetCrs.Srid(0));
        assertThat(geom.crs().orElseThrow().epsgCode())
                .as("srid:0 denotes an undefined CRS and exposes no EPSG code")
                .isEmpty();
    }

    @Test
    void readsGeographyWithProjjsonKeyCrs() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeUnionVariantHeader(out, GEOGRAPHY_VARIANT);
        writeStringField(out, 1, "projjson:my-registry-key");
        out.write(0);
        out.write(0);

        LogicalType.Geography geog = (LogicalType.Geography) read(out);
        assertThat(geog.crs().orElseThrow())
                .as("a projjson:key native crs should parse to ParquetCrs.ProjjsonKey")
                .isEqualTo(new ParquetCrs.ProjjsonKey("my-registry-key"));
    }

    @Test
    void malformedProjjsonDegradesToEmptyCrs() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeUnionVariantHeader(out, GEOMETRY_VARIANT);
        writeStringField(out, 1, "{not json");
        out.write(0);
        out.write(0);

        LogicalType lt = read(out);
        assertThat(lt).isInstanceOf(LogicalType.Geometry.class);
        assertThat(((LogicalType.Geometry) lt).crs())
                .as("malformed PROJJSON must not fail the footer read; CRS surfaces as Optional.empty")
                .isEmpty();
    }

    @Test
    void absentCrsSurfacesAsEmpty() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeUnionVariantHeader(out, GEOMETRY_VARIANT);
        out.write(0); // STOP immediately - no CRS field
        out.write(0);

        LogicalType lt = read(out);
        assertThat(lt).isInstanceOf(LogicalType.Geometry.class);
        assertThat(((LogicalType.Geometry) lt).crs())
                .as("Geometry with no crs field on the wire should surface as Optional.empty")
                .isEmpty();
    }

    // --- helpers ---

    private static LogicalType read(ByteArrayOutputStream bytes) throws IOException {
        return LogicalTypeDeserializer.read(new CompactProtocolReader(new ByteArrayInputStream(bytes.toByteArray())));
    }

    /**
     * Writes the LogicalType union's outer field header for case {@code fieldId}. Field ids 17 / 18 don't fit in the
     * 4-bit delta nibble, so the long form (top nibble = 0) is used followed by the zigzag-i32 absolute id.
     */
    private static void writeUnionVariantHeader(ByteArrayOutputStream out, int fieldId) {
        out.write(TYPE_STRUCT); // top nibble = 0 (long form), low nibble = STRUCT
        writeVarint(out, zigZag32(fieldId));
    }

    /** Writes a struct field carrying a UTF-8 string value (Thrift BINARY = varint length + bytes). */
    private static void writeStringField(ByteArrayOutputStream out, int delta, String value) {
        out.write((delta << 4) | TYPE_BINARY);
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        writeVarint(out, bytes.length);
        out.write(bytes, 0, bytes.length);
    }

    private static int zigZag32(int value) {
        return (value << 1) ^ (value >> 31);
    }

    private static void writeVarint(ByteArrayOutputStream out, int value) {
        long v = value & 0xffffffffL;
        while ((v & ~0x7fL) != 0) {
            out.write((int) ((v & 0x7f) | 0x80));
            v >>>= 7;
        }
        out.write((int) v);
    }
}
