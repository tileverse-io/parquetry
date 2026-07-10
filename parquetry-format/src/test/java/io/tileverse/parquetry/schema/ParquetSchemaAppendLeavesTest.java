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
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

class ParquetSchemaAppendLeavesTest {

    private static ParquetSchema oneColumnSchema() {
        SchemaNode.Primitive value = new SchemaNode.Primitive(
                "value", Repetition.OPTIONAL, PrimitiveKind.DOUBLE, OptionalInt.empty(), Optional.empty(), 1);
        SchemaNode.Group root = new SchemaNode.Group("root", Repetition.REQUIRED, List.of(value), Optional.empty(), 0);
        return new ParquetSchema(root);
    }

    @Test
    void appendsTopLevelLeaves() {
        SchemaNode.Primitive year = new SchemaNode.Primitive(
                "year", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), 2);
        ParquetSchema augmented = oneColumnSchema().withAppendedLeaves(List.of(year));
        assertThat(augmented.leafColumns()).containsExactly(ColumnPath.of("value"), ColumnPath.of("year"));
    }

    @Test
    void appendingEmptyReturnsEquivalentSchema() {
        ParquetSchema base = oneColumnSchema();
        assertThat(base.withAppendedLeaves(List.of()).leafColumns()).isEqualTo(base.leafColumns());
    }
}
