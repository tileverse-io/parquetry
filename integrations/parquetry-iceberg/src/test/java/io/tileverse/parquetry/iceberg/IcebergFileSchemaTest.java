/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.iceberg.IcebergFileSchema.FileColumn;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class IcebergFileSchemaTest {

    @Test
    void mapsEachTopLevelPrimitiveByItsFieldId() {
        ParquetSchema schema = schemaOf(
                primitive("id", PrimitiveKind.INT64, Optional.empty(), 1),
                primitive("name", PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.StringType()), 2));

        IcebergFileSchema fileSchema = IcebergFileSchema.of(schema);

        assertThat(fileSchema.hasFieldIds()).isTrue();

        FileColumn id = fileSchema.byFieldId(1).orElseThrow();
        assertThat(id.path()).isEqualTo(ColumnPath.of("id"));
        assertThat(id.kind()).isEqualTo(PrimitiveKind.INT64);
        assertThat(id.logicalType()).isEmpty();

        FileColumn name = fileSchema.byFieldId(2).orElseThrow();
        assertThat(name.path()).isEqualTo(ColumnPath.of("name"));
        assertThat(name.kind()).isEqualTo(PrimitiveKind.BYTE_ARRAY);
        assertThat(name.logicalType()).contains(new LogicalType.StringType());
    }

    @Test
    void mixedIdFileKeepsEmbeddedIdsAndIgnoresTheMapping() {
        ParquetSchema schema = schemaOf(
                primitive("id", PrimitiveKind.INT64, Optional.empty(), 1),
                primitive("legacy", PrimitiveKind.INT32, Optional.empty(), -1));
        IcebergNameMapping mapping = IcebergNameMapping.fromJson("[{\"field-id\": 9, \"names\": [\"legacy\"]}]");

        IcebergFileSchema fileSchema = IcebergFileSchema.of(schema, mapping);

        assertThat(fileSchema.hasFieldIds()).isTrue();
        assertThat(fileSchema.byFieldId(1)).isPresent();
        assertThat(fileSchema.byFieldId(9)).isEmpty();
        assertThat(fileSchema.byName("legacy")).isPresent();
    }

    @Test
    void synthesizesIdsForAnIdlessFileThroughTheMapping() {
        ParquetSchema schema = schemaOf(
                primitive("id", PrimitiveKind.INT64, Optional.empty(), -1),
                primitive("n", PrimitiveKind.INT32, Optional.empty(), -1));
        IcebergNameMapping mapping = IcebergNameMapping.fromJson(
                "[{\"field-id\": 1, \"names\": [\"id\"]}, {\"field-id\": 2, \"names\": [\"legacy_n\", \"n\"]}]");

        IcebergFileSchema fileSchema = IcebergFileSchema.of(schema, mapping);

        assertThat(fileSchema.hasFieldIds()).isTrue();
        assertThat(fileSchema.byFieldId(1).orElseThrow().path()).isEqualTo(ColumnPath.of("id"));
        assertThat(fileSchema.byFieldId(2).orElseThrow().path()).isEqualTo(ColumnPath.of("n"));
    }

    @Test
    void anUnresolvedIdlessLeafStaysOutOfTheIdIndex() {
        ParquetSchema schema = schemaOf(
                primitive("id", PrimitiveKind.INT64, Optional.empty(), -1),
                primitive("extra", PrimitiveKind.INT32, Optional.empty(), -1));
        IcebergNameMapping mapping = IcebergNameMapping.fromJson("[{\"field-id\": 1, \"names\": [\"id\"]}]");

        IcebergFileSchema fileSchema = IcebergFileSchema.of(schema, mapping);

        assertThat(fileSchema.hasFieldIds()).isTrue();
        assertThat(fileSchema.byFieldId().values()).extracting(FileColumn::path).containsExactly(ColumnPath.of("id"));
        assertThat(fileSchema.byName("extra")).isPresent();
    }

    @Test
    void reportsIdLessWhenNothingResolves() {
        ParquetSchema schema = schemaOf(primitive("id", PrimitiveKind.INT64, Optional.empty(), -1));

        IcebergFileSchema fileSchema = IcebergFileSchema.of(schema);

        assertThat(fileSchema.hasFieldIds()).isFalse();
        assertThat(fileSchema.byName("id")).isPresent();
    }

    @Test
    void reportsIdLessWhenSchemaHasNoTopLevelPrimitives() {
        ParquetSchema schema = schemaOf();

        IcebergFileSchema fileSchema = IcebergFileSchema.of(schema);

        assertThat(fileSchema.hasFieldIds()).isFalse();
        assertThat(fileSchema.byFieldId().keySet()).isEmpty();
    }

    @Test
    void ignoresNestedGroupChildrenInTheIdMap() {
        SchemaNode.Group struct = new SchemaNode.Group(
                "address",
                Repetition.OPTIONAL,
                List.of(primitive("city", PrimitiveKind.BYTE_ARRAY, Optional.empty(), 99)),
                Optional.empty(),
                3);
        ParquetSchema schema = schemaWith(List.of(
                primitive("id", PrimitiveKind.INT64, Optional.empty(), 1),
                struct,
                primitive("name", PrimitiveKind.BYTE_ARRAY, Optional.empty(), 2)));

        IcebergFileSchema fileSchema = IcebergFileSchema.of(schema);

        assertThat(fileSchema.byFieldId().keySet()).containsExactlyInAnyOrder(1, 2);
        assertThat(fileSchema.byFieldId(99)).isEmpty();
        assertThat(fileSchema.byFieldId(3)).isEmpty();
        assertThat(fileSchema.hasFieldIds()).isTrue();
    }

    @Test
    void byNameHitsAndMisses() {
        ParquetSchema schema = schemaOf(primitive("id", PrimitiveKind.INT64, Optional.empty(), 1));

        IcebergFileSchema fileSchema = IcebergFileSchema.of(schema);

        assertThat(fileSchema.byName("id")).isPresent();
        assertThat(fileSchema.byName("missing")).isEmpty();
    }

    private static ParquetSchema schemaOf(SchemaNode... children) {
        return schemaWith(List.of(children));
    }

    private static ParquetSchema schemaWith(List<SchemaNode> children) {
        SchemaNode.Group root = new SchemaNode.Group("root", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive primitive(
            String name, PrimitiveKind kind, Optional<LogicalType> logicalType, int fieldId) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), logicalType, fieldId);
    }
}
