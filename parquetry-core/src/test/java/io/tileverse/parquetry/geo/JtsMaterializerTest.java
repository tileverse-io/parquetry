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
package io.tileverse.parquetry.geo;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.HashMap;
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

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DefaultParquetRecordBatch;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.Validity;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.ParquetCrs;
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

        Map<ColumnPath, Object> out = materializer.materialize(schema, rowOf(schema, Map.of(ID, 42, GEOM, wkb)));
        assertThat(out).containsEntry(ID, 42);
        Geometry decoded = (Geometry) out.get(GEOM);
        assertThat(decoded.equalsExact(pt)).isTrue();
    }

    @Test
    void rowWithoutGeometryAnnotationPassesThroughTheRawBinary() {
        // No logical type on geometry: the materializer doesn't decode WKB; the row's value stays raw binary.
        ParquetSchema schema = schemaWithGeometryAt("geometry", null);
        GeometryFactory gf = new GeometryFactory();
        Point pt = gf.createPoint(new Coordinate(0.0, 0.0));
        MemorySegment wkb = MemorySegment.ofArray(new WKBWriter().write(pt)).asReadOnly();

        JtsMaterializer materializer = new JtsMaterializer(schema);
        assertThat(materializer.geometryColumns()).isEmpty();

        Map<ColumnPath, Object> out = materializer.materialize(schema, rowOf(schema, Map.of(GEOM, wkb)));
        assertThat(out.get(GEOM)).isInstanceOf(MemorySegment.class);
        assertThat(((MemorySegment) out.get(GEOM)).toArray(JAVA_BYTE)).isEqualTo(wkb.toArray(JAVA_BYTE));
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

        Map<ColumnPath, Object> out = materializer.materialize(schema, rowOf(schema, Map.of(GEOM, wkb)));
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
        ParquetSchema schema =
                schemaWithGeometryAt("geometry", new LogicalType.Geometry(Optional.of(ParquetCrs.inline(epsg3857))));
        GeometryFactory gf = new GeometryFactory();
        Point pt = gf.createPoint(new Coordinate(0.0, 0.0));
        MemorySegment wkb = MemorySegment.ofArray(new WKBWriter().write(pt)).asReadOnly();

        JtsMaterializer materializer = new JtsMaterializer(schema);
        assertThat(materializer.sridFor(GEOM))
                .as("EPSG:3857 should be discovered at construction time and exposed via sridFor")
                .hasValue(3857);

        Map<ColumnPath, Object> out = materializer.materialize(schema, rowOf(schema, Map.of(GEOM, wkb)));
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
                .as("absent CRS should be reported as empty rather than guessing 4326")
                .isEmpty();

        Map<ColumnPath, Object> out = materializer.materialize(schema, rowOf(schema, Map.of(GEOM, wkb)));
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
        ParquetSchema schema =
                schemaWithGeometryAt("geometry", new LogicalType.Geometry(Optional.of(ParquetCrs.inline(nameOnly))));

        JtsMaterializer materializer = new JtsMaterializer(schema);
        assertThat(materializer.sridFor(GEOM))
                .as("CRS without an Identifier should not yield an SRID")
                .isEmpty();
    }

    @Test
    void authorityCodeNativeCrsYieldsEpsgSrid() {
        ParquetSchema schema = schemaWithGeometryAt(
                "geometry", new LogicalType.Geometry(Optional.of(new ParquetCrs.AuthorityCode("EPSG", "3857"))));
        JtsMaterializer materializer = new JtsMaterializer(schema);
        assertThat(materializer.sridFor(GEOM))
                .as("an EPSG:3857 native crs reference should stamp SRID 3857 without a registry lookup")
                .hasValue(3857);
    }

    @Test
    void sridZeroNativeCrsLeavesJtsDefaultSrid() {
        ParquetSchema schema =
                schemaWithGeometryAt("geometry", new LogicalType.Geometry(Optional.of(new ParquetCrs.Srid(0))));
        JtsMaterializer materializer = new JtsMaterializer(schema);
        assertThat(materializer.sridFor(GEOM))
                .as("srid:0 denotes an undefined CRS and stamps no SRID")
                .isEmpty();
    }

    // --- helpers ---

    private static ParquetSchema schemaWithGeometryAt(String geometryColumnName, LogicalType geometryAnnotation) {
        SchemaNode.Primitive idField = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive geomField = new SchemaNode.Primitive(
                geometryColumnName,
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.ofNullable(geometryAnnotation),
                -1);
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(idField, geomField), Optional.empty(), -1));
    }

    private static ParquetRecord rowOf(ParquetSchema schema, Map<ColumnPath, Object> values) {
        Map<ColumnPath, ColumnVector> columns = new HashMap<>();
        values.forEach((path, value) -> columns.put(path, vectorOf(value)));
        DefaultParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, columns, 1, Arena.ofConfined());
        return batch.materialize(0);
    }

    private static ColumnVector vectorOf(Object value) {
        Validity present = Validity.allValid(1);
        return switch (value) {
            case Integer i -> IntVector.materialized(new int[] {i}, present);
            case MemorySegment seg -> BinaryVector.materialized(new MemorySegment[] {seg}, present);
            default -> throw new IllegalArgumentException("unsupported test value: " + value);
        };
    }
}
