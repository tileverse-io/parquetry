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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class BboxCoveringsTest {

    private static final ColumnPath GEOM = ColumnPath.of("geom");
    private static final ColumnPath OTHER = ColumnPath.of("other");

    @Test
    void declaredCoveringIsFound() {
        ParquetSchema schema = schemaOf(List.of("geom"), Optional.of("geom_bbox"));
        GeoParquetMetadata geo = GeoParquetMetadata.parse(declaringGeoJson("geom_bbox"));

        Optional<BboxCovering> covering = BboxCoverings.coveringOf(GEOM, schema, geo);

        assertThat(covering).map(BboxCovering::xmin).contains(ColumnPath.of("geom_bbox", "xmin"));
    }

    // A declaration whose leaves the schema lacks names no covering; the conventional struct does not stand in.
    @Test
    void declaredCoveringNeedsLeaves() {
        ParquetSchema schema = schemaOf(List.of("geom"), Optional.of("bbox"));
        GeoParquetMetadata geo = GeoParquetMetadata.parse(declaringGeoJson("geom_bbox"));

        assertThat(BboxCoverings.coveringOf(GEOM, schema, geo)).isEmpty();
    }

    @Test
    void structCoversThePrimary() {
        ParquetSchema schema = schemaOf(List.of("geom", "other"), Optional.of("bbox"));
        GeoParquetMetadata geo = GeoParquetMetadata.parse(plainGeoJson("geom"));

        assertThat(BboxCoverings.coveringOf(GEOM, schema, geo)).contains(BboxCovering.conventional());
        assertThat(BboxCoverings.coveringOf(OTHER, schema, geo)).isEmpty();
    }

    @Test
    void noCoveringWithoutAGeoColumn() {
        ParquetSchema schema = schemaOf(List.of("geom"), Optional.of("bbox"));
        GeoParquetMetadata geo = GeoParquetMetadata.parse(plainGeoJson("geom"));

        assertThat(BboxCoverings.coveringOf(ColumnPath.of("absent"), schema, geo))
                .isEmpty();
    }

    /** Geo metadata for {@code geom} and {@code other}, {@code geom} the primary one, with no covering declared. */
    private static String plainGeoJson(String primary) {
        return """
                {"version":"1.0.0","primary_column":"%s","columns":{\
                "geom":{"encoding":"WKB","geometry_types":[]},\
                "other":{"encoding":"WKB","geometry_types":[]}}}""".formatted(primary);
    }

    /** Geo metadata for {@code geom} alone, declaring its covering under the struct named {@code struct}. */
    private static String declaringGeoJson(String struct) {
        return """
                {"version":"1.1.0","primary_column":"geom","columns":{"geom":{"encoding":"WKB",\
                "geometry_types":[],"covering":{"bbox":{"xmin":["%1$s","xmin"],"ymin":["%1$s","ymin"],\
                "xmax":["%1$s","xmax"],"ymax":["%1$s","ymax"]}}}}}""".formatted(struct);
    }

    /** A schema with the WKB columns named by {@code geometries} and a struct of four box leaves when one is named. */
    private static ParquetSchema schemaOf(List<String> geometries, Optional<String> struct) {
        List<SchemaNode> fields = new ArrayList<>();
        for (String geometry : geometries) {
            fields.add(
                    leaf(geometry, PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.Geometry(Optional.empty()))));
        }
        struct.ifPresent(name -> fields.add(boxStruct(name)));
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, fields, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Group boxStruct(String name) {
        List<SchemaNode> leaves = List.of(
                leaf("xmin", PrimitiveKind.DOUBLE, Optional.empty()),
                leaf("ymin", PrimitiveKind.DOUBLE, Optional.empty()),
                leaf("xmax", PrimitiveKind.DOUBLE, Optional.empty()),
                leaf("ymax", PrimitiveKind.DOUBLE, Optional.empty()));
        return new SchemaNode.Group(name, Repetition.OPTIONAL, leaves, Optional.empty(), -1);
    }

    private static SchemaNode.Primitive leaf(String name, PrimitiveKind kind, Optional<LogicalType> logicalType) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), logicalType, -1);
    }
}
