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
package io.tileverse.parquetry.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.apache.arrow.flatbuf.Field;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.Type;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class ArrowSchemaEncoderTest {

    private static ParquetSchema schema(SchemaNode.Primitive... leaves) {
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(leaves), Optional.empty(), -1));
    }

    @Test
    void encodesIntAndStringFieldsNullableLittleEndian() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), 0);
        SchemaNode.Primitive name = new SchemaNode.Primitive(
                "name",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                1);
        ParquetSchema schema = schema(id, name);

        ByteBuffer buf = ArrowSchemaEncoder.encode(schema, GeoArrowFields.resolve(schema, Optional.empty()));

        Message message = Message.getRootAsMessage(buf);
        assertThat(message.headerType()).isEqualTo(MessageHeader.Schema);
        org.apache.arrow.flatbuf.Schema arrowSchema =
                (org.apache.arrow.flatbuf.Schema) message.header(new org.apache.arrow.flatbuf.Schema());
        assertThat(arrowSchema.fieldsLength()).isEqualTo(2);
        Field idField = arrowSchema.fields(0);
        assertThat(idField.name()).isEqualTo("id");
        assertThat(idField.nullable()).isTrue();
        assertThat(idField.typeType()).isEqualTo(Type.Int);
        Field nameField = arrowSchema.fields(1);
        assertThat(nameField.typeType()).isEqualTo(Type.Utf8);
    }

    @Test
    void attachesGeoArrowExtensionMetadataToGeometryField() {
        SchemaNode.Primitive geom = new SchemaNode.Primitive(
                "geometry",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.Geometry(Optional.empty())),
                0);
        ParquetSchema schema = schema(geom);

        ByteBuffer buf = ArrowSchemaEncoder.encode(schema, GeoArrowFields.resolve(schema, Optional.empty()));

        Message message = Message.getRootAsMessage(buf);
        org.apache.arrow.flatbuf.Schema arrowSchema =
                (org.apache.arrow.flatbuf.Schema) message.header(new org.apache.arrow.flatbuf.Schema());
        Field geomField = arrowSchema.fields(0);
        assertThat(geomField.typeType()).isEqualTo(Type.Binary);
        boolean hasExtensionName = false;
        for (int i = 0; i < geomField.customMetadataLength(); i++) {
            KeyValue kv = geomField.customMetadata(i);
            if ("ARROW:extension:name".equals(kv.key())) {
                assertThat(kv.value()).isEqualTo("geoarrow.wkb");
                hasExtensionName = true;
            }
        }
        assertThat(hasExtensionName).isTrue();
    }
}
