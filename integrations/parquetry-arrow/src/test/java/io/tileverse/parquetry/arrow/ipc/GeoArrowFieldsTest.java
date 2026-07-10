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
package io.tileverse.parquetry.arrow.ipc;

import static org.assertj.core.api.Assertions.assertThat;

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

class GeoArrowFieldsTest {

    @Test
    void detectsGeometryFromLogicalTypeAndEmitsExtensionName() {
        SchemaNode.Primitive geom = new SchemaNode.Primitive(
                "geometry",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.Geometry(Optional.empty())),
                0);
        ParquetSchema schema = new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(geom), Optional.empty(), -1));

        GeoArrowFields fields = GeoArrowFields.resolve(schema, Optional.empty());

        assertThat(fields.isGeometry(ColumnPath.of("geometry"))).isTrue();
        assertThat(fields.metadataFor(ColumnPath.of("geometry"))).containsEntry("ARROW:extension:name", "geoarrow.wkb");
        assertThat(fields.metadataFor(ColumnPath.of("geometry")).get("ARROW:extension:metadata"))
                .contains("\"edges\":\"planar\"");
    }

    @Test
    void nonGeometryColumnHasNoMetadata() {
        SchemaNode.Primitive name = new SchemaNode.Primitive(
                "name",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                0);
        ParquetSchema schema = new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(name), Optional.empty(), -1));

        GeoArrowFields fields = GeoArrowFields.resolve(schema, Optional.empty());

        assertThat(fields.isGeometry(ColumnPath.of("name"))).isFalse();
        assertThat(fields.metadataFor(ColumnPath.of("name"))).isEmpty();
    }
}
