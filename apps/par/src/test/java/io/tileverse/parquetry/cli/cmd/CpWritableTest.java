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
package io.tileverse.parquetry.cli.cmd;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.cli.UnsupportedSchemaException;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class CpWritableTest {

    @Test
    void flatSchemaIsWritable() {
        assertThatCode(() -> CpWritable.requireWritable(flat())).doesNotThrowAnyException();
    }

    @Test
    void structSchemaIsWritable() {
        assertThatCode(() -> CpWritable.requireWritable(struct())).doesNotThrowAnyException();
    }

    @Test
    void int96IsRejectedAtTopLevel() {
        assertThatThrownBy(() -> CpWritable.requireWritable(int96Top()))
                .isInstanceOf(UnsupportedSchemaException.class)
                .hasMessageContaining("INT96")
                .hasMessageContaining("ts");
    }

    @Test
    void int96IsRejectedInsideAStruct() {
        assertThatThrownBy(() -> CpWritable.requireWritable(int96InStruct()))
                .isInstanceOf(UnsupportedSchemaException.class)
                .hasMessageContaining("INT96")
                .hasMessageContaining("bbox.ts");
    }

    private static ParquetSchema flat() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        return new ParquetSchema(
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id), Optional.empty(), -1));
    }

    private static ParquetSchema struct() {
        SchemaNode.Primitive xmin = new SchemaNode.Primitive(
                "xmin", Repetition.REQUIRED, PrimitiveKind.FLOAT, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group bbox = new SchemaNode.Group("bbox", Repetition.REQUIRED, List.of(xmin), Optional.empty(), -1);
        return new ParquetSchema(
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(bbox), Optional.empty(), -1));
    }

    private static ParquetSchema int96Top() {
        SchemaNode.Primitive ts = new SchemaNode.Primitive(
                "ts", Repetition.OPTIONAL, PrimitiveKind.INT96, OptionalInt.empty(), Optional.empty(), -1);
        return new ParquetSchema(
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(ts), Optional.empty(), -1));
    }

    private static ParquetSchema int96InStruct() {
        SchemaNode.Primitive ts = new SchemaNode.Primitive(
                "ts", Repetition.OPTIONAL, PrimitiveKind.INT96, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group bbox = new SchemaNode.Group("bbox", Repetition.REQUIRED, List.of(ts), Optional.empty(), -1);
        return new ParquetSchema(
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(bbox), Optional.empty(), -1));
    }
}
