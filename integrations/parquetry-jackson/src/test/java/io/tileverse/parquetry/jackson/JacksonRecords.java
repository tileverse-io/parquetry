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

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/** Schema-node and value builders shared by the encoder and materializer tests. */
final class JacksonRecords {

    private JacksonRecords() {}

    static SchemaNode.Group root(SchemaNode... fields) {
        return new SchemaNode.Group("root", Repetition.REQUIRED, java.util.List.of(fields), Optional.empty(), -1);
    }

    static SchemaNode primitive(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), Optional.empty(), -1);
    }

    static SchemaNode stringLeaf(String name) {
        return new SchemaNode.Primitive(
                name,
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
    }

    static SchemaNode uuidLeaf(String name) {
        return new SchemaNode.Primitive(
                name,
                Repetition.OPTIONAL,
                PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                OptionalInt.of(16),
                Optional.of(new LogicalType.UuidType()),
                -1);
    }

    static SchemaNode.Group struct(String name, SchemaNode... children) {
        return new SchemaNode.Group(name, Repetition.OPTIONAL, java.util.List.of(children), Optional.empty(), -1);
    }

    static SchemaNode.Group list(String name, SchemaNode element) {
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, java.util.List.of(element), Optional.empty(), -1);
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, java.util.List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
    }

    static SchemaNode.Group map(String name, SchemaNode key, SchemaNode value) {
        SchemaNode.Group keyValue = new SchemaNode.Group(
                "key_value", Repetition.REPEATED, java.util.List.of(key, value), Optional.empty(), -1);
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, java.util.List.of(keyValue), Optional.of(new LogicalType.MapType()), -1);
    }

    static SchemaNode.Group variant(String name) {
        return new SchemaNode.Group(
                name,
                Repetition.OPTIONAL,
                java.util.List.of(
                        primitive("metadata", PrimitiveKind.BYTE_ARRAY), primitive("value", PrimitiveKind.BYTE_ARRAY)),
                Optional.of(new LogicalType.Variant()),
                -1);
    }

    static Validity validBits(int size) {
        return Validity.allValid(size);
    }

    static MemorySegment utf8(String text) {
        return MemorySegment.ofArray(text.getBytes(StandardCharsets.UTF_8));
    }

    static MemorySegment bytes(byte[] raw) {
        return MemorySegment.ofArray(raw);
    }
}
