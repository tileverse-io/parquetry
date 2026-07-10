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
package io.tileverse.parquetry.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.LogicalType;

class GroupKindTest {

    private static final int NO_FIELD_ID = -1;

    @Test
    void variantAnnotatedGroupIsVariant() {
        SchemaNode.Group group = annotatedGroup(new LogicalType.Variant());

        assertThat(GroupKind.of(group)).isEqualTo(GroupKind.VARIANT);
    }

    @Test
    void listAnnotatedGroupIsList() {
        SchemaNode.Group group = annotatedGroup(new LogicalType.ListType());

        assertThat(GroupKind.of(group)).isEqualTo(GroupKind.LIST);
    }

    @Test
    void mapAnnotatedGroupIsMap() {
        SchemaNode.Group group = annotatedGroup(new LogicalType.MapType());

        assertThat(GroupKind.of(group)).isEqualTo(GroupKind.MAP);
    }

    @Test
    void bareRepeatedGroupIsList() {
        SchemaNode.Group group =
                new SchemaNode.Group("element", Repetition.REPEATED, List.of(), Optional.empty(), NO_FIELD_ID);

        assertThat(GroupKind.of(group)).isEqualTo(GroupKind.LIST);
    }

    @Test
    void plainGroupIsStruct() {
        SchemaNode.Group group =
                new SchemaNode.Group("record", Repetition.OPTIONAL, List.of(), Optional.empty(), NO_FIELD_ID);

        assertThat(GroupKind.of(group)).isEqualTo(GroupKind.STRUCT);
    }

    private static SchemaNode.Group annotatedGroup(LogicalType logicalType) {
        return new SchemaNode.Group("annotated", Repetition.OPTIONAL, List.of(), Optional.of(logicalType), NO_FIELD_ID);
    }
}
