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
package io.tileverse.parquetry.arrow.cdi;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.arrow.ipc.ArrowField;
import io.tileverse.parquetry.arrow.ipc.LogicalColumns;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Exports an {@link ArrowField} tree into an {@code ArrowSchema} over the C Data Interface and imports it back with
 * arrow-java's {@code Data.importField}, an independent C Data consumer, to confirm the format strings, names,
 * children, and extension metadata reconstruct the intended Arrow types.
 */
class ArrowSchemaExportIT {

    @Test
    void exportsAScalarInt64() {
        Field field = exportAndImport(leaf("id", PrimitiveKind.INT64, Optional.empty()));
        assertThat(field.getName()).isEqualTo("id");
        assertThat(field.getType()).isInstanceOfSatisfying(ArrowType.Int.class, integer -> {
            assertThat(integer.getBitWidth()).isEqualTo(64);
            assertThat(integer.getIsSigned()).isTrue();
        });
    }

    @Test
    void exportsAListOfInt32() {
        Field field = exportAndImport(listGroup("nums", leaf("element", PrimitiveKind.INT32, Optional.empty())));
        assertThat(field.getType()).isInstanceOf(ArrowType.List.class);
        assertThat(field.getChildren()).hasSize(1);
        assertThat(field.getChildren().get(0).getName()).isEqualTo("element");
    }

    @Test
    void exportsAStruct() {
        Field field = exportAndImport(structGroup(
                "person",
                leaf("age", PrimitiveKind.INT32, Optional.empty()),
                leaf("name", PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.StringType()))));
        assertThat(field.getType()).isInstanceOf(ArrowType.Struct.class);
        assertThat(field.getChildren()).extracting(Field::getName).containsExactly("age", "name");
    }

    @Test
    void exportsAMap() {
        Field field = exportAndImport(mapGroup(
                "props",
                leaf("key", PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.StringType())),
                leaf("value", PrimitiveKind.INT64, Optional.empty())));
        assertThat(field.getType()).isInstanceOf(ArrowType.Map.class);
        assertThat(field.getChildren()).hasSize(1);
    }

    @Test
    void exportsAVariantAsAStructWithTheExtensionTag() {
        Field field = exportAndImport(variantGroup("v"));
        assertThat(field.getType()).isInstanceOf(ArrowType.Struct.class);
        assertThat(field.getChildren()).extracting(Field::getName).containsExactly("metadata", "value");
        assertThat(field.getMetadata()).containsEntry("ARROW:extension:name", "arrow.variant");
    }

    private static Field exportAndImport(SchemaNode field) {
        ParquetSchema schema = new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(field), Optional.empty(), -1));
        ArrowField arrowField =
                LogicalColumns.of(schema, Optional.empty()).get(0).field();
        try (Arena arena = Arena.ofConfined();
                RootAllocator allocator = new RootAllocator();
                CDataDictionaryProvider provider = new CDataDictionaryProvider()) {
            MemorySegment exported = ArrowSchemaExporter.export(arrowField, arena);
            try (ArrowSchema imported = ArrowSchema.wrap(exported.address())) {
                return Data.importField(allocator, imported, provider);
            }
        }
    }

    private static SchemaNode.Primitive leaf(String name, PrimitiveKind kind, Optional<LogicalType> logical) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), logical, 0);
    }

    private static SchemaNode.Group listGroup(String name, SchemaNode element) {
        SchemaNode.Group wrapper =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, List.of(wrapper), Optional.of(new LogicalType.ListType()), 1);
    }

    private static SchemaNode.Group structGroup(String name, SchemaNode... fields) {
        return new SchemaNode.Group(name, Repetition.OPTIONAL, List.of(fields), Optional.empty(), 1);
    }

    private static SchemaNode.Group mapGroup(String name, SchemaNode key, SchemaNode value) {
        SchemaNode.Group keyValue =
                new SchemaNode.Group("key_value", Repetition.REPEATED, List.of(key, value), Optional.empty(), -1);
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, List.of(keyValue), Optional.of(new LogicalType.MapType()), 1);
    }

    private static SchemaNode.Group variantGroup(String name) {
        SchemaNode.Primitive metadata = leaf("metadata", PrimitiveKind.BYTE_ARRAY, Optional.empty());
        SchemaNode.Primitive value = leaf("value", PrimitiveKind.BYTE_ARRAY, Optional.empty());
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, List.of(metadata, value), Optional.of(new LogicalType.Variant()), 1);
    }
}
