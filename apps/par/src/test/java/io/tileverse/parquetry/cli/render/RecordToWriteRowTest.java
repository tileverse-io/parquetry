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
package io.tileverse.parquetry.cli.render;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.cli.support.Fixtures;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class RecordToWriteRowTest {

    @Test
    void flatPrimitiveSchemaIsSupported() {
        RecordToWriteRow.requireWritable(Fixtures.citiesSchema());
        assertThat(true).isTrue();
    }

    @Test
    void nestedGroupIsRejected() {
        SchemaNode.Group inner = new SchemaNode.Group("addr", Repetition.OPTIONAL, List.of(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(inner), Optional.empty(), -1);
        assertThatThrownBy(() -> RecordToWriteRow.requireWritable(new ParquetSchema(root)))
                .hasMessageContaining("flat");
    }

    @Test
    void int96IsRejected() {
        SchemaNode.Primitive ts = new SchemaNode.Primitive(
                "ts", Repetition.OPTIONAL, PrimitiveKind.INT96, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(ts), Optional.empty(), -1);
        assertThatThrownBy(() -> RecordToWriteRow.requireWritable(new ParquetSchema(root)))
                .hasMessageContaining("INT96");
    }
}
