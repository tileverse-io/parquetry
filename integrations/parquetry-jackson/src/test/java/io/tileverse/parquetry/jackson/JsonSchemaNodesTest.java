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
package io.tileverse.parquetry.jackson;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class JsonSchemaNodesTest {

    private static SchemaNode primitive(String name, PrimitiveKind kind, Optional<LogicalType> logicalType) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), logicalType, -1);
    }

    private static SchemaNode.Group group(String name, List<SchemaNode> children, Optional<LogicalType> logicalType) {
        return new SchemaNode.Group(name, Repetition.OPTIONAL, children, logicalType, -1);
    }

    @Test
    void classifiesByLogicalTypeThenRepetition() {
        SchemaNode.Group variant = group(
                "v",
                List.of(
                        primitive("metadata", PrimitiveKind.BYTE_ARRAY, Optional.empty()),
                        primitive("value", PrimitiveKind.BYTE_ARRAY, Optional.empty())),
                Optional.of(new LogicalType.Variant()));
        SchemaNode.Group list = group("l", List.of(), Optional.of(new LogicalType.ListType()));
        SchemaNode.Group map = group("m", List.of(), Optional.of(new LogicalType.MapType()));
        SchemaNode.Group struct = group("s", List.of(), Optional.empty());
        SchemaNode.Group legacyList = new SchemaNode.Group("ll", Repetition.REPEATED, List.of(), Optional.empty(), -1);

        assertThat(JsonSchemaNodes.classify(variant)).isEqualTo(JsonSchemaNodes.Kind.VARIANT);
        assertThat(JsonSchemaNodes.classify(list)).isEqualTo(JsonSchemaNodes.Kind.LIST);
        assertThat(JsonSchemaNodes.classify(map)).isEqualTo(JsonSchemaNodes.Kind.MAP);
        assertThat(JsonSchemaNodes.classify(struct)).isEqualTo(JsonSchemaNodes.Kind.STRUCT);
        assertThat(JsonSchemaNodes.classify(legacyList)).isEqualTo(JsonSchemaNodes.Kind.LIST);
    }

    @Test
    void resolvesThreeLevelAndTwoLevelListElement() {
        SchemaNode element = primitive("element", PrimitiveKind.INT32, Optional.empty());
        SchemaNode.Group repeatedWrapper =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group threeLevel = group("l", List.of(repeatedWrapper), Optional.of(new LogicalType.ListType()));
        assertThat(JsonSchemaNodes.elementNode(threeLevel)).isSameAs(element);

        SchemaNode legacyElement = new SchemaNode.Primitive(
                "element", Repetition.REPEATED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group twoLevel = group("l", List.of(legacyElement), Optional.of(new LogicalType.ListType()));
        assertThat(JsonSchemaNodes.elementNode(twoLevel)).isSameAs(legacyElement);
    }

    @Test
    void resolvesMapKeyAndValueNodes() {
        SchemaNode key = primitive("key", PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.StringType()));
        SchemaNode value = primitive("value", PrimitiveKind.INT32, Optional.empty());
        SchemaNode.Group keyValue =
                new SchemaNode.Group("key_value", Repetition.REPEATED, List.of(key, value), Optional.empty(), -1);
        SchemaNode.Group map = group("m", List.of(keyValue), Optional.of(new LogicalType.MapType()));

        assertThat(JsonSchemaNodes.keyNode(map)).isSameAs(key);
        assertThat(JsonSchemaNodes.valueNode(map)).isSameAs(value);
    }

    @Test
    void detectsStringLikeLogicalTypes() {
        assertThat(JsonSchemaNodes.isStringLike(Optional.of(new LogicalType.StringType())))
                .isTrue();
        assertThat(JsonSchemaNodes.isStringLike(Optional.of(new LogicalType.EnumType())))
                .isTrue();
        assertThat(JsonSchemaNodes.isStringLike(Optional.of(new LogicalType.JsonType())))
                .isTrue();
        assertThat(JsonSchemaNodes.isStringLike(Optional.empty())).isFalse();
    }
}
