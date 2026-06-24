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
package io.tileverse.parquetry.cli.expr;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.cli.support.Fixtures;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

class GeometryColumnsTest {

    @Test
    void emptyWhenNoGeometry() {
        Set<ColumnPath> resolved = GeometryColumns.resolve(Fixtures.citiesSchema(), Map.of());
        assertThat(resolved).isEmpty();
    }

    @Test
    void readsFromGeoMetadata() {
        String geoJson = """
                {"version":"1.0.0","primary_column":"geom",\
                "columns":{"geom":{"encoding":"WKB","geometry_types":[]}}}""";
        Map<String, String> keyValueMetadata = Map.of("geo", geoJson);

        Set<ColumnPath> resolved = GeometryColumns.resolve(Fixtures.citiesSchema(), keyValueMetadata);

        assertThat(resolved).containsExactly(ColumnPath.of("geom"));
    }

    @Test
    void readsFromLogicalType() {
        ParquetSchema schema = schemaWithGeometryLeaf();

        Set<ColumnPath> resolved = GeometryColumns.resolve(schema, Map.of());

        assertThat(resolved).containsExactly(ColumnPath.of("geometry"));
    }

    @Test
    void unionsLogicalTypeAndGeoMetadata() {
        ParquetSchema schema = schemaWithGeometryLeaf();
        String geoJson = """
                {"version":"1.0.0","primary_column":"geom",\
                "columns":{"geom":{"encoding":"WKB","geometry_types":[]}}}""";
        Map<String, String> keyValueMetadata = Map.of("geo", geoJson);

        Set<ColumnPath> resolved = GeometryColumns.resolve(schema, keyValueMetadata);

        assertThat(resolved).containsExactlyInAnyOrder(ColumnPath.of("geometry"), ColumnPath.of("geom"));
    }

    @Test
    void resolvesFromParsedGeoMetadata() {
        ParquetSchema schema = Fixtures.geoCitiesSchema();
        GeoParquetMetadata geo = GeoParquetMetadata.parse("{\"version\":\"1.1.0\",\"primary_column\":\"geometry\","
                + "\"columns\":{\"geometry\":{\"encoding\":\"WKB\",\"geometry_types\":[]}}}");
        Set<ColumnPath> columns = GeometryColumns.resolve(schema, Optional.of(geo));
        assertThat(columns).contains(ColumnPath.of("geometry"));
    }

    @Test
    void resolvesEmptyGeoMetadataFromSchemaOnly() {
        ParquetSchema schema = Fixtures.citiesSchema();
        Set<ColumnPath> columns = GeometryColumns.resolve(schema, Optional.empty());
        assertThat(columns).isEmpty();
    }

    private static ParquetSchema schemaWithGeometryLeaf() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive geometry = new SchemaNode.Primitive(
                "geometry",
                Repetition.REQUIRED,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.Geometry(Optional.empty())),
                -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id, geometry), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
