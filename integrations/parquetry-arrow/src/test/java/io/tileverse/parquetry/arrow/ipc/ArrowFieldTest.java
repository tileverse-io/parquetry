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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.arrow.ipc.ArrowField.Kind;
import io.tileverse.parquetry.arrow.ipc.LogicalColumns.LogicalColumn;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.UnsupportedFeatureException;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class ArrowFieldTest {

    @Test
    void primitiveResolvesToALeafFieldWithNoChildren() {
        SchemaNode.Primitive id = primitive("id", PrimitiveKind.INT64, Optional.empty());

        ArrowField field = ArrowField.of(id, noGeometry(schema(id)));

        assertThat(field.kind()).isEqualTo(Kind.PRIMITIVE);
        assertThat(field.leaf()).isNotNull();
        assertThat(field.leaf().kind()).isEqualTo(ArrowFieldType.Kind.INT);
        assertThat(field.children()).isEmpty();
        assertThat(field.name()).isEqualTo("id");
    }

    @Test
    void threeLevelListResolvesToOneElementChild() {
        SchemaNode.Primitive element = primitive("element", PrimitiveKind.INT32, Optional.empty());
        SchemaNode.Group listWrapper =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group tags = new SchemaNode.Group(
                "tags", Repetition.OPTIONAL, List.of(listWrapper), Optional.of(new LogicalType.ListType()), 1);

        ArrowField field = ArrowField.of(tags, noGeometry(schema(tags)));

        assertThat(field.kind()).isEqualTo(Kind.LIST);
        assertThat(field.children()).hasSize(1);
        ArrowField item = field.children().get(0);
        assertThat(item.kind()).isEqualTo(Kind.PRIMITIVE);
        assertThat(item.name()).isEqualTo("element");
    }

    @Test
    void structResolvesToOneChildPerFieldInOrder() {
        SchemaNode.Primitive a = primitive("a", PrimitiveKind.INT32, Optional.empty());
        SchemaNode.Primitive b = primitive("b", PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.StringType()));
        SchemaNode.Group addr = new SchemaNode.Group("addr", Repetition.OPTIONAL, List.of(a, b), Optional.empty(), 1);

        ArrowField field = ArrowField.of(addr, noGeometry(schema(addr)));

        assertThat(field.kind()).isEqualTo(Kind.STRUCT);
        assertThat(field.children()).extracting(ArrowField::name).containsExactly("a", "b");
    }

    @Test
    void mapResolvesToAKeyValueEntryStruct() {
        SchemaNode.Primitive key =
                primitive("key", PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.StringType()));
        SchemaNode.Primitive value = primitive("value", PrimitiveKind.INT64, Optional.empty());
        SchemaNode.Group keyValue =
                new SchemaNode.Group("key_value", Repetition.REPEATED, List.of(key, value), Optional.empty(), -1);
        SchemaNode.Group map = new SchemaNode.Group(
                "m", Repetition.OPTIONAL, List.of(keyValue), Optional.of(new LogicalType.MapType()), 1);

        ArrowField field = ArrowField.of(map, noGeometry(schema(map)));

        assertThat(field.kind()).isEqualTo(Kind.MAP);
        assertThat(field.children()).hasSize(1);
        ArrowField entries = field.children().get(0);
        assertThat(entries.kind()).isEqualTo(Kind.STRUCT);
        assertThat(entries.name()).isEqualTo("entries");
        assertThat(entries.nullable()).isFalse();
        assertThat(entries.children()).extracting(ArrowField::name).containsExactly("key", "value");
    }

    @Test
    void variantResolvesToAMetadataValueStructWithTheExtensionTag() {
        SchemaNode.Primitive metadata = primitive("metadata", PrimitiveKind.BYTE_ARRAY, Optional.empty());
        SchemaNode.Primitive value = primitive("value", PrimitiveKind.BYTE_ARRAY, Optional.empty());
        SchemaNode.Group variant = new SchemaNode.Group(
                "v", Repetition.OPTIONAL, List.of(metadata, value), Optional.of(new LogicalType.Variant()), 1);

        ArrowField field = ArrowField.of(variant, noGeometry(schema(variant)));

        assertThat(field.kind()).isEqualTo(Kind.VARIANT);
        assertThat(field.children()).extracting(ArrowField::name).containsExactly("metadata", "value");
        assertThat(field.children())
                .allSatisfy(child -> assertThat(child.leaf().kind()).isEqualTo(ArrowFieldType.Kind.BINARY));
        assertThat(field.extensionMetadata()).containsKey("ARROW:extension:name");
    }

    @Test
    void logicalColumnsReturnsOneEntryPerTopLevelFieldWithCompositesCollapsed() {
        SchemaNode.Primitive id = primitive("id", PrimitiveKind.INT64, Optional.empty());
        SchemaNode.Primitive a = primitive("a", PrimitiveKind.INT32, Optional.empty());
        SchemaNode.Group addr = new SchemaNode.Group("addr", Repetition.OPTIONAL, List.of(a), Optional.empty(), 1);
        ParquetSchema schema = schema(id, addr);

        List<LogicalColumn> columns = LogicalColumns.of(schema, noGeometry(schema));

        assertThat(columns).extracting(column -> column.path().dot()).containsExactly("id", "addr");
        assertThat(columns).extracting(column -> column.field().kind()).containsExactly(Kind.PRIMITIVE, Kind.STRUCT);
    }

    @Test
    void resolvingRejectsAVariantNestedUnderAList() {
        SchemaNode.Group variant = variantGroup("element");
        SchemaNode.Group listWrapper =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(variant), Optional.empty(), -1);
        SchemaNode.Group tags = new SchemaNode.Group(
                "tags", Repetition.OPTIONAL, List.of(listWrapper), Optional.of(new LogicalType.ListType()), 1);
        ParquetSchema schema = schema(tags);
        GeoArrowFields geometry = noGeometry(schema);

        assertThatThrownBy(() -> LogicalColumns.of(schema, geometry))
                .isInstanceOf(UnsupportedFeatureException.class)
                .hasMessageContaining("Variant nested under a list or map");
    }

    @Test
    void resolvingRejectsAVariantNestedUnderAMapValue() {
        SchemaNode.Primitive key =
                primitive("key", PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.StringType()));
        SchemaNode.Group value = variantGroup("value");
        SchemaNode.Group keyValue =
                new SchemaNode.Group("key_value", Repetition.REPEATED, List.of(key, value), Optional.empty(), -1);
        SchemaNode.Group map = new SchemaNode.Group(
                "m", Repetition.OPTIONAL, List.of(keyValue), Optional.of(new LogicalType.MapType()), 1);
        ParquetSchema schema = schema(map);
        GeoArrowFields geometry = noGeometry(schema);

        assertThatThrownBy(() -> LogicalColumns.of(schema, geometry))
                .isInstanceOf(UnsupportedFeatureException.class)
                .hasMessageContaining("Variant nested under a list or map");
    }

    @Test
    void resolvingAcceptsAVariantNestedUnderAStruct() {
        SchemaNode.Group variant = variantGroup("v");
        SchemaNode.Group struct =
                new SchemaNode.Group("rec", Repetition.OPTIONAL, List.of(variant), Optional.empty(), 1);
        ParquetSchema schema = schema(struct);

        assertThatCode(() -> LogicalColumns.of(schema, noGeometry(schema))).doesNotThrowAnyException();
    }

    @Test
    void resolvingAcceptsATopLevelVariant() {
        SchemaNode.Group variant = variantGroup("v");
        ParquetSchema schema = schema(variant);

        assertThatCode(() -> LogicalColumns.of(schema, noGeometry(schema))).doesNotThrowAnyException();
    }

    private static SchemaNode.Group variantGroup(String name) {
        SchemaNode.Primitive metadata = primitive("metadata", PrimitiveKind.BYTE_ARRAY, Optional.empty());
        SchemaNode.Primitive value = primitive("value", PrimitiveKind.BYTE_ARRAY, Optional.empty());
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, List.of(metadata, value), Optional.of(new LogicalType.Variant()), 1);
    }

    private static SchemaNode.Primitive primitive(String name, PrimitiveKind kind, Optional<LogicalType> logical) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), logical, 0);
    }

    private static ParquetSchema schema(SchemaNode... fields) {
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(fields), Optional.empty(), -1));
    }

    private static GeoArrowFields noGeometry(ParquetSchema schema) {
        return GeoArrowFields.resolve(schema, Optional.empty());
    }
}
