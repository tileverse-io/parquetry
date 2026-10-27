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

import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class BoundingBoxColumnTest {

    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");

    @Test
    void duckDbOrderHasNoDefect() {
        ParquetSchema schema = schemaWithBbox(fields(PrimitiveKind.DOUBLE, "xmin", "xmax", "ymin", "ymax"));

        assertThat(BoundingBoxColumn.defect(schema, GEOMETRY)).isEmpty();
    }

    @Test
    void scalarBboxIsADefect() {
        SchemaNode bbox = primitive("bbox", PrimitiveKind.DOUBLE);
        ParquetSchema schema = schemaOf(geometry(), bbox);

        assertThat(BoundingBoxColumn.defect(schema, GEOMETRY)).contains("the 'bbox' column is not a struct");
    }

    @Test
    void integerFieldIsADefect() {
        List<SchemaNode> fields = fields(PrimitiveKind.DOUBLE, "xmin", "ymin", "xmax");
        fields.add(primitive("ymax", PrimitiveKind.INT64));
        ParquetSchema schema = schemaWithBbox(fields);

        assertThat(BoundingBoxColumn.defect(schema, GEOMETRY)).contains("the field 'bbox.ymax' is not FLOAT or DOUBLE");
    }

    @Test
    void inSpecOrderMovesOnlyFields() {
        ParquetSchema schema = schemaWithBbox(fields(PrimitiveKind.FLOAT, "xmax", "xmin", "ymax", "ymin"));

        ParquetSchema reordered = BoundingBoxColumn.inSpecOrder(schema);

        assertThat(reordered.leafColumns())
                .extracting(ColumnPath::dot)
                .containsExactly("geometry", "bbox.xmin", "bbox.ymin", "bbox.xmax", "bbox.ymax");
        assertThat(reordered.find(ColumnPath.of("bbox")).orElseThrow().repetition())
                .isEqualTo(Repetition.OPTIONAL);
    }

    private static List<SchemaNode> fields(PrimitiveKind kind, String... names) {
        List<SchemaNode> fields = new ArrayList<>();
        for (String name : names) {
            fields.add(primitive(name, kind));
        }
        return fields;
    }

    private static ParquetSchema schemaWithBbox(List<SchemaNode> fields) {
        SchemaNode.Group bbox = new SchemaNode.Group("bbox", Repetition.OPTIONAL, fields, Optional.empty(), -1);
        return schemaOf(geometry(), bbox);
    }

    private static ParquetSchema schemaOf(SchemaNode... columns) {
        return new ParquetSchema(
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(columns), Optional.empty(), -1));
    }

    private static SchemaNode geometry() {
        return primitive("geometry", PrimitiveKind.BYTE_ARRAY);
    }

    private static SchemaNode.Primitive primitive(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), Optional.empty(), -1);
    }
}
