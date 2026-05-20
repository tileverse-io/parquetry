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
package io.tileverse.parquetry.geo.jts;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKBWriter;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.materializer.RowAccessor;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;
import io.tileverse.parquetry.schema.geo.projjson.GeographicCRS;
import io.tileverse.parquetry.schema.geo.projjson.Identifier;
import io.tileverse.parquetry.schema.geo.projjson.ProjectedCRS;

class JtsMaterializerTest {

    private static final ColumnPath GEOM = ColumnPath.of("geometry");
    private static final ColumnPath ID = ColumnPath.of("id");

    @Test
    void rowWithGeometryColumnEmitsJtsPoint() {
        ParquetSchema schema = schemaWithGeometryAt("geometry", new LogicalType.Geometry(Optional.empty()));
        GeometryFactory gf = new GeometryFactory();
        Point pt = gf.createPoint(new Coordinate(1.5, 2.5));
        MemorySegment wkb = MemorySegment.ofArray(new WKBWriter().write(pt)).asReadOnly();

        JtsMaterializer materializer = new JtsMaterializer(schema);
        assertThat(materializer.geometryColumns()).containsExactly(GEOM);

        Map<ColumnPath, Object> out = materializer.materialize(schema, rowOf(Map.of(ID, 42, GEOM, wkb)));
        assertThat(out).containsEntry(ID, 42);
        Geometry decoded = (Geometry) out.get(GEOM);
        assertThat(decoded.equalsExact(pt)).isTrue();
    }

    @Test
    void rowWithoutGeometryAnnotationPassesThroughByteBuffer() {
        // No logical type on geometry: materializer doesn't decode WKB; the row's value stays as ByteBuffer.
        ParquetSchema schema = schemaWithGeometryAt("geometry", null);
        GeometryFactory gf = new GeometryFactory();
        Point pt = gf.createPoint(new Coordinate(0.0, 0.0));
        MemorySegment wkb = MemorySegment.ofArray(new WKBWriter().write(pt)).asReadOnly();

        JtsMaterializer materializer = new JtsMaterializer(schema);
        assertThat(materializer.geometryColumns()).isEmpty();

        Map<ColumnPath, Object> out = materializer.materialize(schema, rowOf(Map.of(GEOM, wkb)));
        assertThat(out.get(GEOM)).isSameAs(wkb);
    }

    @Test
    void geographyAnnotationAlsoDecodes() {
        ParquetSchema schema =
                schemaWithGeometryAt("geometry", new LogicalType.Geography(Optional.empty(), Optional.empty()));
        GeometryFactory gf = new GeometryFactory();
        Point pt = gf.createPoint(new Coordinate(-71.0589, 42.3601));
        MemorySegment wkb = MemorySegment.ofArray(new WKBWriter().write(pt)).asReadOnly();

        JtsMaterializer materializer = new JtsMaterializer(schema);
        assertThat(materializer.geometryColumns()).containsExactly(GEOM);

        Map<ColumnPath, Object> out = materializer.materialize(schema, rowOf(Map.of(GEOM, wkb)));
        assertThat(out.get(GEOM)).isInstanceOf(Point.class);
        Point decoded = (Point) out.get(GEOM);
        assertThat(decoded.getX()).isEqualTo(-71.0589);
        assertThat(decoded.getY()).isEqualTo(42.3601);
    }

    @Test
    void decodedGeometryCarriesEpsgSridDerivedFromTypedCrs() {
        CoordinateReferenceSystem epsg3857 = new ProjectedCRS(
                Optional.of("WGS 84 / Pseudo-Mercator"),
                Optional.of(new Identifier("EPSG", "3857", Optional.empty(), Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        ParquetSchema schema = schemaWithGeometryAt("geometry", new LogicalType.Geometry(Optional.of(epsg3857)));
        GeometryFactory gf = new GeometryFactory();
        Point pt = gf.createPoint(new Coordinate(0.0, 0.0));
        MemorySegment wkb = MemorySegment.ofArray(new WKBWriter().write(pt)).asReadOnly();

        JtsMaterializer materializer = new JtsMaterializer(schema);
        assertThat(materializer.sridFor(GEOM))
                .as("EPSG:3857 should be discovered at construction time and exposed via sridFor")
                .hasValue(3857);

        Map<ColumnPath, Object> out = materializer.materialize(schema, rowOf(Map.of(GEOM, wkb)));
        Geometry decoded = (Geometry) out.get(GEOM);
        assertThat(decoded.getSRID())
                .as("decoded geometry should carry the EPSG SRID derived from its typed CRS")
                .isEqualTo(3857);
    }

    @Test
    void absentCrsLeavesJtsDefaultSrid() {
        // Optional.empty() on the typed CRS means "use the GeoParquet spec default" (OGC:CRS84); the materializer
        // intentionally does not assume 4326 - consumers that want 4326 stamp it themselves.
        ParquetSchema schema = schemaWithGeometryAt("geometry", new LogicalType.Geometry(Optional.empty()));
        GeometryFactory gf = new GeometryFactory();
        Point pt = gf.createPoint(new Coordinate(0.0, 0.0));
        MemorySegment wkb = MemorySegment.ofArray(new WKBWriter().write(pt)).asReadOnly();

        JtsMaterializer materializer = new JtsMaterializer(schema);
        assertThat(materializer.sridFor(GEOM))
                .as("absent CRS should surface as empty rather than guessing 4326")
                .isEmpty();

        Map<ColumnPath, Object> out = materializer.materialize(schema, rowOf(Map.of(GEOM, wkb)));
        Geometry decoded = (Geometry) out.get(GEOM);
        assertThat(decoded.getSRID())
                .as("absent CRS should leave JTS's default SRID (0)")
                .isZero();
    }

    @Test
    void crsWithoutEpsgIdentifierLeavesSridUnset() {
        // GeographicCRS without an id() - i.e. an inline PROJJSON definition with no authority block. The materializer
        // should not invent an SRID from the name alone.
        CoordinateReferenceSystem nameOnly =
                new GeographicCRS(Optional.of("Some Local CRS"), Optional.empty(), Optional.empty(), Optional.empty());
        ParquetSchema schema = schemaWithGeometryAt("geometry", new LogicalType.Geometry(Optional.of(nameOnly)));

        JtsMaterializer materializer = new JtsMaterializer(schema);
        assertThat(materializer.sridFor(GEOM))
                .as("CRS without an Identifier should not yield an SRID")
                .isEmpty();
    }

    // --- helpers ---

    private static ParquetSchema schemaWithGeometryAt(String geometryColumnName, LogicalType geometryAnnotation) {
        Field.Primitive idField = new Field.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        Field.Primitive geomField = new Field.Primitive(
                geometryColumnName,
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.ofNullable(geometryAnnotation),
                -1);
        return new ParquetSchema(
                new Field.Group("root", Repetition.REQUIRED, List.of(idField, geomField), Optional.empty(), -1));
    }

    private static RowAccessor rowOf(Map<ColumnPath, Object> values) {
        return new RowAccessor() {
            @Override
            public Object get(ColumnPath path) {
                return values.get(path);
            }

            @Override
            public boolean isGroupNull(ColumnPath path) {
                return false;
            }

            @Override
            public Map<ColumnPath, Object> values() {
                return values;
            }
        };
    }
}
