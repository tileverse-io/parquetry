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
package io.tileverse.parquetry.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.LogicalType;

/**
 * Resolves logical column paths to physical leaf paths across struct, LIST, and MAP shapes, descending the synthetic
 * {@code list}/{@code element} and {@code key_value} levels by structure rather than by literal name.
 */
class ParquetSchemaResolveTest {

    private final ParquetSchema schema = sampleSchema();

    @Test
    void resolvesPlainStructPathUnchanged() {
        ResolvedColumn resolved =
                schema.resolve(ColumnPath.of("brand", "names", "primary")).orElseThrow();

        assertThat(resolved.physical()).isEqualTo(ColumnPath.of("brand", "names", "primary"));
        assertThat(resolved.isRepeated()).isFalse();
        assertThat(resolved.repeatedPhysicalIndices()).isEmpty();
        assertThat(resolved.kind()).isEqualTo(PrimitiveKind.BYTE_ARRAY);
    }

    @Test
    void resolvesListElementByDescendingSyntheticLevels() {
        ResolvedColumn resolved =
                schema.resolve(ColumnPath.of("addresses", "locality")).orElseThrow();

        assertThat(resolved.physical()).isEqualTo(ColumnPath.of("addresses", "list", "element", "locality"));
        assertThat(resolved.isRepeated()).isTrue();
        assertThat(resolved.repeatedPhysicalIndices()).isEqualTo(List.of(1));
    }

    @Test
    void resolvesAlreadyPhysicalListPath() {
        ResolvedColumn resolved = schema.resolve(ColumnPath.of("addresses", "list", "element", "locality"))
                .orElseThrow();

        assertThat(resolved.physical()).isEqualTo(ColumnPath.of("addresses", "list", "element", "locality"));
        assertThat(resolved.isRepeated()).isTrue();
        assertThat(resolved.repeatedPhysicalIndices()).isEqualTo(List.of(1));
    }

    @Test
    void resolvesMapValueByDescendingKeyValue() {
        ResolvedColumn resolved = schema.resolve(ColumnPath.of("tags", "value")).orElseThrow();

        assertThat(resolved.physical()).isEqualTo(ColumnPath.of("tags", "key_value", "value"));
        assertThat(resolved.isRepeated()).isTrue();
        assertThat(resolved.repeatedPhysicalIndices()).isEqualTo(List.of(1));
    }

    @Test
    void resolvesMapKeyByDescendingKeyValue() {
        ResolvedColumn resolved = schema.resolve(ColumnPath.of("tags", "key")).orElseThrow();

        assertThat(resolved.physical()).isEqualTo(ColumnPath.of("tags", "key_value", "key"));
        assertThat(resolved.isRepeated()).isTrue();
        assertThat(resolved.repeatedPhysicalIndices()).isEqualTo(List.of(1));
    }

    @Test
    void physicalToLogicalDropsSyntheticLevels() {
        assertThat(schema.physicalToLogical(ColumnPath.of("addresses", "list", "element", "locality")))
                .isEqualTo(ColumnPath.of("addresses", "locality"));
        assertThat(schema.physicalToLogical(ColumnPath.of("tags", "key_value", "value")))
                .isEqualTo(ColumnPath.of("tags", "value"));
        assertThat(schema.physicalToLogical(ColumnPath.of("brand", "names", "primary")))
                .isEqualTo(ColumnPath.of("brand", "names", "primary"));
    }

    @Test
    void returnsEmptyForUnknownChild() {
        assertThat(schema.resolve(ColumnPath.of("brand", "nope"))).isEmpty();
    }

    private static ParquetSchema sampleSchema() {
        SchemaNode brand = struct("brand", struct("names", string("primary")));
        SchemaNode addresses = list("addresses", struct("element", string("locality")));
        SchemaNode tags = map("tags", string("key"), string("value"));
        SchemaNode.Group root = group("root", Repetition.REQUIRED, List.of(brand, addresses, tags));
        return new ParquetSchema(root);
    }

    private static SchemaNode struct(String name, SchemaNode... children) {
        return group(name, Repetition.OPTIONAL, List.of(children));
    }

    private static SchemaNode list(String name, SchemaNode elementGroup) {
        SchemaNode.Group repeatedList = group("list", Repetition.REPEATED, List.of(elementGroup));
        return group(name, Repetition.OPTIONAL, List.of(repeatedList));
    }

    private static SchemaNode map(String name, SchemaNode key, SchemaNode value) {
        SchemaNode.Group keyValue = group("key_value", Repetition.REPEATED, List.of(key, value));
        return group(name, Repetition.OPTIONAL, List.of(keyValue));
    }

    private static SchemaNode.Group group(String name, Repetition repetition, List<SchemaNode> children) {
        return new SchemaNode.Group(name, repetition, children, Optional.empty(), -1);
    }

    private static SchemaNode string(String name) {
        return new SchemaNode.Primitive(
                name,
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
    }
}
