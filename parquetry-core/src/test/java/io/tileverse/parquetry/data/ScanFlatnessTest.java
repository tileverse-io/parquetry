/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class ScanFlatnessTest {

    @Test
    void aSchemaOfPlainColumnsIsAllFlat() {
        SchemaNode.Primitive v = required("v");
        ParquetSchema schema = new ParquetSchema(
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(v), Optional.empty(), -1));

        assertThat(ParquetReader.allFlat(schema, schema.leafColumns())).isTrue();
    }

    @Test
    void aSchemaWithARepeatedLeafIsNotFlat() {
        SchemaNode.Primitive v = required("v");
        SchemaNode.Primitive element = required("element");
        SchemaNode.Group tags =
                new SchemaNode.Group("tags", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(v, tags), Optional.empty(), -1));

        assertThat(ParquetReader.allFlat(schema, schema.leafColumns())).isFalse();
    }

    private static SchemaNode.Primitive required(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }
}
