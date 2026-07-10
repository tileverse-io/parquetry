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
package io.tileverse.parquetry.cli.render;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

/**
 * Verifies that {@code --columns} accepts logical paths into LIST and struct shapes, resolving each to the physical
 * leaf the file stores, while plain physical and group paths keep their existing behavior.
 */
class ProjectionsLogicalPathTest {

    private final ParquetSchema schema = sampleSchema();

    @Test
    void logicalListElementResolvesToPhysicalLeaf() {
        Projections.Resolved resolved = Projections.resolve(List.of("addresses.locality"), schema);

        assertThat(resolved.keptLeaves()).containsExactly(ColumnPath.of("addresses", "list", "element", "locality"));
    }

    @Test
    void structPathExpandsToItsLeaf() {
        Projections.Resolved resolved = Projections.resolve(List.of("brand.names.primary"), schema);

        assertThat(resolved.keptLeaves()).containsExactly(ColumnPath.of("brand", "names", "primary"));
    }

    @Test
    void flatLeafKeepsItsPath() {
        Projections.Resolved resolved = Projections.resolve(List.of("pop"), schema);

        assertThat(resolved.keptLeaves()).containsExactly(ColumnPath.of("pop"));
    }

    @Test
    void unknownLogicalPathIsRejected() {
        assertThatThrownBy(() -> Projections.resolve(List.of("addresses.nope"), schema))
                .hasMessageContaining("unknown column");
    }

    private static ParquetSchema sampleSchema() {
        SchemaNode pop = primitive("pop", PrimitiveKind.INT64, Optional.empty());
        SchemaNode brand = struct("brand", struct("names", string("primary")));
        SchemaNode addresses = list("addresses", struct("element", string("locality")));
        SchemaNode.Group root = group("root", Repetition.REQUIRED, List.of(pop, brand, addresses));
        return new ParquetSchema(root);
    }

    private static SchemaNode struct(String name, SchemaNode... children) {
        return group(name, Repetition.OPTIONAL, List.of(children));
    }

    private static SchemaNode list(String name, SchemaNode elementGroup) {
        SchemaNode.Group repeatedList = group("list", Repetition.REPEATED, List.of(elementGroup));
        return group(name, Repetition.OPTIONAL, List.of(repeatedList));
    }

    private static SchemaNode.Group group(String name, Repetition repetition, List<SchemaNode> children) {
        return new SchemaNode.Group(name, repetition, children, Optional.empty(), -1);
    }

    private static SchemaNode string(String name) {
        return primitive(name, PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.StringType()));
    }

    private static SchemaNode primitive(String name, PrimitiveKind kind, Optional<LogicalType> logicalType) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), logicalType, -1);
    }
}
