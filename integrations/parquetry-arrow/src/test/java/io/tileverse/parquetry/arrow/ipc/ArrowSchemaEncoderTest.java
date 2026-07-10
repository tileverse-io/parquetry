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

    @Test
    void encodesNestedListStructMapAndVariantFieldsRecursively() {
        ParquetSchema schema =
                groupSchema(listGroup("tags"), structGroup("addr"), mapGroup("props"), variantGroup("v"));

        ByteBuffer buf = ArrowSchemaEncoder.encode(schema, GeoArrowFields.resolve(schema, Optional.empty()));

        Message message = Message.getRootAsMessage(buf);
        org.apache.arrow.flatbuf.Schema arrow =
                (org.apache.arrow.flatbuf.Schema) message.header(new org.apache.arrow.flatbuf.Schema());
        assertThat(arrow.fieldsLength()).isEqualTo(4);

        Field listField = arrow.fields(0);
        assertThat(listField.typeType()).isEqualTo(Type.List);
        assertThat(listField.childrenLength()).isEqualTo(1);
        assertThat(listField.children(0).name()).isEqualTo("element");
        assertThat(listField.children(0).typeType()).isEqualTo(Type.Int);

        Field structField = arrow.fields(1);
        assertThat(structField.typeType()).isEqualTo(Type.Struct_);
        assertThat(structField.childrenLength()).isEqualTo(2);
        assertThat(structField.children(0).name()).isEqualTo("a");
        assertThat(structField.children(1).typeType()).isEqualTo(Type.Utf8);

        Field mapField = arrow.fields(2);
        assertThat(mapField.typeType()).isEqualTo(Type.Map);
        assertThat(mapField.childrenLength()).isEqualTo(1);
        Field entries = mapField.children(0);
        assertThat(entries.typeType()).isEqualTo(Type.Struct_);
        assertThat(entries.childrenLength()).isEqualTo(2);
        assertThat(entries.children(0).name()).isEqualTo("key");
        assertThat(entries.children(1).name()).isEqualTo("value");

        Field variantField = arrow.fields(3);
        assertThat(variantField.typeType()).isEqualTo(Type.Struct_);
        assertThat(variantField.childrenLength()).isEqualTo(2);
        assertThat(extensionName(variantField)).isEqualTo("arrow.variant");

        // arrow-java accepts the nested schema, a second independent check that the flatbuffer is well formed.
        org.apache.arrow.vector.types.pojo.Schema pojo =
                org.apache.arrow.vector.ipc.message.MessageSerializer.deserializeSchema(message);
        assertThat(pojo.getFields()).hasSize(4);
    }

    private static String extensionName(Field field) {
        for (int i = 0; i < field.customMetadataLength(); i++) {
            KeyValue kv = field.customMetadata(i);
            if ("ARROW:extension:name".equals(kv.key())) {
                return kv.value();
            }
        }
        return null;
    }

    private static ParquetSchema groupSchema(SchemaNode... fields) {
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(fields), Optional.empty(), -1));
    }

    private static SchemaNode.Primitive leaf(String name, Repetition repetition, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, repetition, kind, OptionalInt.empty(), Optional.empty(), 0);
    }

    private static SchemaNode.Group listGroup(String name) {
        SchemaNode.Primitive element = leaf("element", Repetition.OPTIONAL, PrimitiveKind.INT32);
        SchemaNode.Group wrapper =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, List.of(wrapper), Optional.of(new LogicalType.ListType()), 1);
    }

    private static SchemaNode.Group structGroup(String name) {
        SchemaNode.Primitive a = leaf("a", Repetition.OPTIONAL, PrimitiveKind.INT32);
        SchemaNode.Primitive b = new SchemaNode.Primitive(
                "b",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                1);
        return new SchemaNode.Group(name, Repetition.OPTIONAL, List.of(a, b), Optional.empty(), 2);
    }

    private static SchemaNode.Group mapGroup(String name) {
        SchemaNode.Primitive key = new SchemaNode.Primitive(
                "key",
                Repetition.REQUIRED,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                0);
        SchemaNode.Primitive value = leaf("value", Repetition.OPTIONAL, PrimitiveKind.INT64);
        SchemaNode.Group keyValue =
                new SchemaNode.Group("key_value", Repetition.REPEATED, List.of(key, value), Optional.empty(), -1);
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, List.of(keyValue), Optional.of(new LogicalType.MapType()), 2);
    }

    private static SchemaNode.Group variantGroup(String name) {
        SchemaNode.Primitive metadata = leaf("metadata", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY);
        SchemaNode.Primitive value = leaf("value", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY);
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, List.of(metadata, value), Optional.of(new LogicalType.Variant()), 2);
    }
}
