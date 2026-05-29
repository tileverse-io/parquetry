/*
 * Copyright (c) 2026 Multivers.io
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.GeospatialStatistics;
import io.tileverse.parquetry.format.ParquetFormatException;

/**
 * Direct unit coverage for {@link GeospatialStatisticsDeserializer} and {@link BoundingBoxDeserializer}: synthesizes
 * Thrift-compact bytes by hand so the test is independent of any encoder. Exercises bbox-only, types-only,
 * both-together, antimeridian-wrap, optional Z/M axes, forward-compat fields, and the four required-field rejections on
 * {@link BoundingBox}.
 */
class GeospatialStatisticsDeserializerTest {

    private static final int TYPE_I32 = 5;
    private static final int TYPE_DOUBLE = 7;
    private static final int TYPE_LIST = 9;
    private static final int TYPE_STRUCT = 12;

    @Test
    void readsBboxOnly() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeField(out, 1, TYPE_STRUCT);
        writeXyBbox(out, -180.0, 180.0, -90.0, 90.0);
        out.write(0);
        GeospatialStatistics stats = read(out);
        assertThat(stats.bbox()).isPresent();
        BoundingBox bbox = stats.bbox().orElseThrow();
        assertThat(bbox.xmin()).isEqualTo(-180.0);
        assertThat(bbox.xmax()).isEqualTo(180.0);
        assertThat(bbox.ymin()).isEqualTo(-90.0);
        assertThat(bbox.ymax()).isEqualTo(90.0);
        assertThat(bbox.zmin()).isEmpty();
        assertThat(bbox.zmax()).isEmpty();
        assertThat(bbox.mmin()).isEmpty();
        assertThat(bbox.mmax()).isEmpty();
        assertThat(bbox.wrapsAntimeridian()).isFalse();
        assertThat(stats.geospatialTypes()).isEmpty();
    }

    @Test
    void readsTypesOnly() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeField(out, 2, TYPE_LIST);
        writeI32List(out, 1, 3, 6);
        out.write(0);
        GeospatialStatistics stats = read(out);
        assertThat(stats.bbox()).isEmpty();
        assertThat(stats.geospatialTypes()).contains(List.of(1, 3, 6));
    }

    @Test
    void readsBboxAndTypes() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeField(out, 1, TYPE_STRUCT);
        writeXyBbox(out, -10.0, 10.0, -5.0, 5.0);
        // delta 1 from the bbox (outer lastFieldId == 1) to land on field 2.
        writeField(out, 1, TYPE_LIST);
        writeI32List(out, 1, 1001, 3001);
        out.write(0);
        GeospatialStatistics stats = read(out);
        assertThat(stats.bbox()).isPresent();
        assertThat(stats.geospatialTypes()).contains(List.of(1, 1001, 3001));
    }

    @Test
    void readsEmptyStruct() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0);
        GeospatialStatistics stats = read(out);
        assertThat(stats.bbox()).isEmpty();
        assertThat(stats.geospatialTypes()).isEmpty();
    }

    @Test
    void readsBboxWithZ() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeField(out, 1, TYPE_STRUCT);
        writeXyzBbox(out, -1.0, 1.0, -2.0, 2.0, 0.0, 100.0);
        out.write(0);
        BoundingBox bbox = read(out).bbox().orElseThrow();
        assertThat(bbox.zmin()).hasValue(0.0);
        assertThat(bbox.zmax()).hasValue(100.0);
        assertThat(bbox.mmin()).isEmpty();
        assertThat(bbox.mmax()).isEmpty();
    }

    @Test
    void readsBboxWithZAndM() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeField(out, 1, TYPE_STRUCT);
        writeXyzmBbox(out, -1.0, 1.0, -2.0, 2.0, 0.0, 100.0, 1700000000.0, 1800000000.0);
        out.write(0);
        BoundingBox bbox = read(out).bbox().orElseThrow();
        assertThat(bbox.zmin()).hasValue(0.0);
        assertThat(bbox.zmax()).hasValue(100.0);
        assertThat(bbox.mmin()).hasValue(1700000000.0);
        assertThat(bbox.mmax()).hasValue(1800000000.0);
    }

    @Test
    void recognizesAntimeridianWrap() throws IOException {
        // GeoParquet 2.0 spec: a bbox covering Fiji wraps the antimeridian: xmin=178, xmax=-175.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeField(out, 1, TYPE_STRUCT);
        writeXyBbox(out, 178.0, -175.0, -20.0, -10.0);
        out.write(0);
        BoundingBox bbox = read(out).bbox().orElseThrow();
        assertThat(bbox.wrapsAntimeridian()).isTrue();
    }

    @Test
    void ignoresForwardCompatField() throws IOException {
        // Writer added a future i32 field at id 99; reader must skip it.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeField(out, 1, TYPE_STRUCT);
        writeXyBbox(out, 0.0, 1.0, 0.0, 1.0);
        // long-form field header for id 99 (delta=0 forces zigzag absolute id).
        out.write(TYPE_I32);
        writeVarint(out, zigZag32(99));
        writeVarint(out, zigZag32(42));
        out.write(0);
        GeospatialStatistics stats = read(out);
        assertThat(stats.bbox()).isPresent();
    }

    @Test
    void rejectsBboxMissingXmin() {
        // Field 1 is the BoundingBox struct; we synthesize a body that starts at field 2 (xmax) so xmin is absent.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeField(out, 1, TYPE_STRUCT);
        writeField(out, 2, TYPE_DOUBLE); // xmax (delta 2 from 0)
        writeDouble(out, 1.0);
        writeField(out, 1, TYPE_DOUBLE); // ymin (delta 1 from 2 -> id 3)
        writeDouble(out, -1.0);
        writeField(out, 1, TYPE_DOUBLE); // ymax (delta 1 from 3 -> id 4)
        writeDouble(out, 1.0);
        out.write(0); // end of BoundingBox
        out.write(0); // end of GeospatialStatistics
        assertThatThrownBy(() -> read(out))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("xmin");
    }

    @Test
    void rejectsBboxMissingXmax() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeField(out, 1, TYPE_STRUCT);
        writeField(out, 1, TYPE_DOUBLE); // xmin
        writeDouble(out, -1.0);
        writeField(out, 2, TYPE_DOUBLE); // delta 2 -> id 3 (ymin)
        writeDouble(out, -1.0);
        writeField(out, 1, TYPE_DOUBLE); // ymax
        writeDouble(out, 1.0);
        out.write(0);
        out.write(0);
        assertThatThrownBy(() -> read(out))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("xmax");
    }

    @Test
    void rejectsBboxMissingYmin() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeField(out, 1, TYPE_STRUCT);
        writeField(out, 1, TYPE_DOUBLE); // xmin
        writeDouble(out, -1.0);
        writeField(out, 1, TYPE_DOUBLE); // xmax
        writeDouble(out, 1.0);
        writeField(out, 2, TYPE_DOUBLE); // delta 2 -> id 4 (ymax), skipping ymin
        writeDouble(out, 1.0);
        out.write(0);
        out.write(0);
        assertThatThrownBy(() -> read(out))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("ymin");
    }

    @Test
    void rejectsBboxMissingYmax() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeField(out, 1, TYPE_STRUCT);
        writeField(out, 1, TYPE_DOUBLE);
        writeDouble(out, -1.0);
        writeField(out, 1, TYPE_DOUBLE);
        writeDouble(out, 1.0);
        writeField(out, 1, TYPE_DOUBLE);
        writeDouble(out, -1.0);
        out.write(0); // end BoundingBox after xmin, xmax, ymin
        out.write(0);
        assertThatThrownBy(() -> read(out))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("ymax");
    }

    // --- helpers ---

    private static GeospatialStatistics read(ByteArrayOutputStream bytes) throws IOException {
        return GeospatialStatisticsDeserializer.read(
                new CompactProtocolReader(new ByteArrayInputStream(bytes.toByteArray())));
    }

    private static void writeField(ByteArrayOutputStream out, int delta, int typeCode) {
        out.write((delta << 4) | typeCode);
    }

    private static void writeXyBbox(ByteArrayOutputStream out, double xmin, double xmax, double ymin, double ymax) {
        writeField(out, 1, TYPE_DOUBLE);
        writeDouble(out, xmin);
        writeField(out, 1, TYPE_DOUBLE);
        writeDouble(out, xmax);
        writeField(out, 1, TYPE_DOUBLE);
        writeDouble(out, ymin);
        writeField(out, 1, TYPE_DOUBLE);
        writeDouble(out, ymax);
        out.write(0);
    }

    private static void writeXyzBbox(
            ByteArrayOutputStream out, double xmin, double xmax, double ymin, double ymax, double zmin, double zmax) {
        writeField(out, 1, TYPE_DOUBLE);
        writeDouble(out, xmin);
        writeField(out, 1, TYPE_DOUBLE);
        writeDouble(out, xmax);
        writeField(out, 1, TYPE_DOUBLE);
        writeDouble(out, ymin);
        writeField(out, 1, TYPE_DOUBLE);
        writeDouble(out, ymax);
        writeField(out, 1, TYPE_DOUBLE);
        writeDouble(out, zmin);
        writeField(out, 1, TYPE_DOUBLE);
        writeDouble(out, zmax);
        out.write(0);
    }

    private static void writeXyzmBbox(
            ByteArrayOutputStream out,
            double xmin,
            double xmax,
            double ymin,
            double ymax,
            double zmin,
            double zmax,
            double mmin,
            double mmax) {
        writeField(out, 1, TYPE_DOUBLE);
        writeDouble(out, xmin);
        writeField(out, 1, TYPE_DOUBLE);
        writeDouble(out, xmax);
        writeField(out, 1, TYPE_DOUBLE);
        writeDouble(out, ymin);
        writeField(out, 1, TYPE_DOUBLE);
        writeDouble(out, ymax);
        writeField(out, 1, TYPE_DOUBLE);
        writeDouble(out, zmin);
        writeField(out, 1, TYPE_DOUBLE);
        writeDouble(out, zmax);
        writeField(out, 1, TYPE_DOUBLE);
        writeDouble(out, mmin);
        writeField(out, 1, TYPE_DOUBLE);
        writeDouble(out, mmax);
        out.write(0);
    }

    private static void writeI32List(ByteArrayOutputStream out, int... values) {
        if (values.length < 15) {
            out.write((values.length << 4) | TYPE_I32);
        } else {
            out.write((0xf << 4) | TYPE_I32);
            writeVarint(out, values.length);
        }
        for (int value : values) {
            writeVarint(out, zigZag32(value));
        }
    }

    private static void writeDouble(ByteArrayOutputStream out, double value) {
        long bits = Double.doubleToLongBits(value);
        for (int i = 0; i < 8; i++) {
            out.write((int) (bits >>> (i * 8)) & 0xff);
        }
    }

    private static int zigZag32(int n) {
        return (n << 1) ^ (n >> 31);
    }

    private static void writeVarint(ByteArrayOutputStream out, int value) {
        while ((value & ~0x7f) != 0) {
            out.write((value & 0x7f) | 0x80);
            value >>>= 7;
        }
        out.write(value & 0x7f);
    }
}
