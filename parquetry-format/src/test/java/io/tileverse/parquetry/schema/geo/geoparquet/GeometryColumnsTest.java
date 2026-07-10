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
package io.tileverse.parquetry.schema.geo.geoparquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class GeometryColumnsTest {

    @Test
    void emptyWhenNoGeometry() {
        Set<ColumnPath> resolved = GeometryColumns.resolve(nonGeometrySchema(), Map.of());
        assertThat(resolved).isEmpty();
    }

    @Test
    void readsFromGeoMetadata() {
        Map<String, String> keyValueMetadata = Map.of("geo", geoJson("geom"));

        Set<ColumnPath> resolved = GeometryColumns.resolve(nonGeometrySchema(), keyValueMetadata);

        assertThat(resolved).containsExactly(ColumnPath.of("geom"));
    }

    @Test
    void readsFromLogicalType() {
        Set<ColumnPath> resolved = GeometryColumns.resolve(schemaWithGeometryLeaf("geometry"), Map.of());

        assertThat(resolved).containsExactly(ColumnPath.of("geometry"));
    }

    @Test
    void unionsLogicalTypeAndGeoMetadata() {
        Map<String, String> keyValueMetadata = Map.of("geo", geoJson("geom"));

        Set<ColumnPath> resolved = GeometryColumns.resolve(schemaWithGeometryLeaf("geometry"), keyValueMetadata);

        assertThat(resolved).containsExactlyInAnyOrder(ColumnPath.of("geometry"), ColumnPath.of("geom"));
    }

    @Test
    void resolvesFromParsedGeoMetadata() {
        GeoParquetMetadata geo = GeoParquetMetadata.parse(geoJson("geom"));

        Set<ColumnPath> resolved = GeometryColumns.resolve(nonGeometrySchema(), Optional.of(geo));

        assertThat(resolved).containsExactly(ColumnPath.of("geom"));
    }

    @Test
    void readsADottedGeoMetadataColumnNameAsANestedPath() {
        Map<String, String> keyValueMetadata = Map.of("geo", geoJson("nested.geom"));

        Set<ColumnPath> resolved = GeometryColumns.resolve(nonGeometrySchema(), keyValueMetadata);

        assertThat(resolved).containsExactly(ColumnPath.of("nested", "geom"));
    }

    @Test
    void primaryPrefersTheDeclaredPrimaryColumnOverTheFirstLeaf() {
        ParquetSchema schema = schemaWithTwoGeometryLeaves("geom_a", "geom_b");
        GeoParquetMetadata geo = GeoParquetMetadata.parse(geoJson("geom_b"));

        Optional<ColumnPath> primary = GeometryColumns.primary(schema, Optional.of(geo));

        assertThat(primary).contains(ColumnPath.of("geom_b"));
    }

    @Test
    void primaryFallsBackToTheFirstLeafWithoutMetadata() {
        ParquetSchema schema = schemaWithTwoGeometryLeaves("geom_a", "geom_b");

        Optional<ColumnPath> primary = GeometryColumns.primary(schema, Optional.empty());

        assertThat(primary).contains(ColumnPath.of("geom_a"));
    }

    @Test
    void primaryEmptyWhenNoGeometry() {
        Optional<ColumnPath> primary = GeometryColumns.primary(nonGeometrySchema(), Optional.empty());

        assertThat(primary).isEmpty();
    }

    private static String geoJson(String primaryColumn) {
        return "{\"version\":\"1.1.0\",\"primary_column\":\"" + primaryColumn + "\",\"columns\":{\"" + primaryColumn
                + "\":{\"encoding\":\"WKB\",\"geometry_types\":[]}}}";
    }

    private static ParquetSchema nonGeometrySchema() {
        SchemaNode.Primitive id = primitive("id", PrimitiveKind.INT32, Optional.empty());
        SchemaNode.Primitive name = primitive("name", PrimitiveKind.BYTE_ARRAY, Optional.empty());
        return schemaOf(id, name);
    }

    private static ParquetSchema schemaWithGeometryLeaf(String geometryName) {
        SchemaNode.Primitive id = primitive("id", PrimitiveKind.INT32, Optional.empty());
        SchemaNode.Primitive geometry = primitive(geometryName, PrimitiveKind.BYTE_ARRAY, geometryType());
        return schemaOf(id, geometry);
    }

    private static ParquetSchema schemaWithTwoGeometryLeaves(String first, String second) {
        SchemaNode.Primitive geometryA = primitive(first, PrimitiveKind.BYTE_ARRAY, geometryType());
        SchemaNode.Primitive geometryB = primitive(second, PrimitiveKind.BYTE_ARRAY, geometryType());
        return schemaOf(geometryA, geometryB);
    }

    private static Optional<LogicalType> geometryType() {
        return Optional.of(new LogicalType.Geometry(Optional.empty()));
    }

    private static SchemaNode.Primitive primitive(String name, PrimitiveKind kind, Optional<LogicalType> logicalType) {
        return new SchemaNode.Primitive(name, Repetition.REQUIRED, kind, OptionalInt.empty(), logicalType, -1);
    }

    private static ParquetSchema schemaOf(SchemaNode.Primitive... leaves) {
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(leaves), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
