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
package io.tileverse.parquetry.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

class ParquetJavaOracleTest {

    @Test
    void flatPrimitivesAndBinaryAndNull() {
        MessageType schema = Types.buildMessage()
                .required(PrimitiveTypeName.INT32)
                .named("i")
                .required(PrimitiveTypeName.INT64)
                .named("l")
                .required(PrimitiveTypeName.DOUBLE)
                .named("d")
                .required(PrimitiveTypeName.BOOLEAN)
                .named("b")
                .required(PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("s")
                .optional(PrimitiveTypeName.INT32)
                .named("opt")
                .named("m");
        SimpleGroup group = new SimpleGroup(schema);
        group.add(0, 7);
        group.add(1, 8L);
        group.add(2, 1.5d);
        group.add(3, true);
        group.add(4, Binary.fromString("hi"));

        Map<String, Object> row = ParquetJavaOracle.canonicalize(group);

        assertThat(row.get("i")).as("int leaf").isEqualTo(7);
        assertThat(row.get("l")).as("long leaf").isEqualTo(8L);
        assertThat(row.get("d")).as("double leaf").isEqualTo(1.5d);
        assertThat(row.get("b")).as("boolean leaf").isEqualTo(true);
        assertThat(row.get("s")).as("binary leaf as ByteBuffer").isEqualTo(ByteBuffer.wrap("hi".getBytes()));
        assertThat(row.get("opt")).as("absent optional is null").isNull();
        assertThat(row.keySet()).as("field order preserved").containsExactly("i", "l", "d", "b", "s", "opt");
    }

    @Test
    void int96LeafBecomesByteBuffer() {
        MessageType schema = Types.buildMessage()
                .required(PrimitiveTypeName.INT96)
                .named("ts")
                .named("m");
        SimpleGroup group = new SimpleGroup(schema);
        byte[] twelve = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
        group.add(0, Binary.fromConstantByteArray(twelve));

        Map<String, Object> row = ParquetJavaOracle.canonicalize(group);

        assertThat(row.get("ts")).as("INT96 leaf as 12-byte ByteBuffer").isEqualTo(ByteBuffer.wrap(twelve));
    }

    @Test
    void nestedStructBecomesNestedMap() {
        MessageType schema = Types.buildMessage()
                .requiredGroup()
                .required(PrimitiveTypeName.INT32)
                .named("x")
                .required(PrimitiveTypeName.INT32)
                .named("y")
                .named("point")
                .named("m");
        SimpleGroup group = new SimpleGroup(schema);
        Group point = group.addGroup(0);
        point.add(0, 3);
        point.add(1, 4);

        Map<String, Object> row = ParquetJavaOracle.canonicalize(group);

        assertThat(row.get("point")).as("struct as nested map").isEqualTo(Map.of("x", 3, "y", 4));
    }

    @Test
    void threeLevelListWithPresentEmptyAndNull() {
        MessageType schema = Types.buildMessage()
                .optionalList()
                .requiredElement(PrimitiveTypeName.INT32)
                .named("nums")
                .named("m");

        SimpleGroup present = new SimpleGroup(schema);
        Group list = present.addGroup(0);
        list.addGroup(0).add(0, 10);
        list.addGroup(0).add(0, 20);
        assertThat(ParquetJavaOracle.canonicalize(present).get("nums"))
                .as("present list")
                .isEqualTo(List.of(10, 20));

        SimpleGroup empty = new SimpleGroup(schema);
        empty.addGroup(0);
        assertThat(ParquetJavaOracle.canonicalize(empty).get("nums"))
                .as("empty list")
                .isEqualTo(List.of());

        SimpleGroup nullList = new SimpleGroup(schema);
        assertThat(ParquetJavaOracle.canonicalize(nullList).get("nums"))
                .as("absent optional list is null")
                .isNull();
    }

    @Test
    void threeLevelListWithAbsentOptionalElement() {
        MessageType schema = Types.buildMessage()
                .optionalList()
                .optionalElement(PrimitiveTypeName.INT32)
                .named("nums")
                .named("m");

        SimpleGroup group = new SimpleGroup(schema);
        Group list = group.addGroup(0);
        list.addGroup(0).add(0, 10);
        list.addGroup(0); // element group present, the optional INT32 left absent -> null element
        list.addGroup(0).add(0, 30);

        assertThat(ParquetJavaOracle.canonicalize(group).get("nums"))
                .as("absent optional element canonicalizes to null")
                .isEqualTo(Arrays.asList(10, null, 30));
    }

    @Test
    void mapWithIntKeysBecomesLinkedHashMap() {
        MessageType schema = Types.buildMessage()
                .requiredMap()
                .key(PrimitiveTypeName.INT32)
                .requiredValue(PrimitiveTypeName.INT32)
                .named("m2i")
                .named("m");
        SimpleGroup group = new SimpleGroup(schema);
        Group map = group.addGroup(0);
        Group e0 = map.addGroup(0);
        e0.add(0, 1);
        e0.add(1, 10);
        Group e1 = map.addGroup(0);
        e1.add(0, 2);
        e1.add(1, 20);

        Object cell = ParquetJavaOracle.canonicalize(group).get("m2i");

        assertThat(cell).as("map cell type").isInstanceOf(Map.class);
        @SuppressWarnings("unchecked")
        Map<Object, Object> entries = (Map<Object, Object>) cell;
        assertThat(entries)
                .as("map entries")
                .containsEntry(1, 10)
                .containsEntry(2, 20)
                .hasSize(2);
    }

    @Test
    void legacyRepeatedPrimitiveBecomesList() {
        MessageType schema = Types.buildMessage()
                .repeated(PrimitiveTypeName.INT32)
                .named("vals")
                .named("m");
        SimpleGroup group = new SimpleGroup(schema);
        group.add(0, 5);
        group.add(0, 6);
        group.add(0, 7);

        assertThat(ParquetJavaOracle.canonicalize(group).get("vals"))
                .as("legacy repeated primitive as list")
                .isEqualTo(List.of(5, 6, 7));
    }
}
