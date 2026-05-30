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
package io.tileverse.parquetry.conformance;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * Reads a parquet fixture into a dependency-free canonical tree using parquet-java's {@code SimpleGroup} reader, the
 * oracle the conformance corpus compares parquetry against. Each row becomes a {@code LinkedHashMap} keyed by field
 * name in schema order; nested lists, maps, and structs become {@code ArrayList}, {@code LinkedHashMap}, and nested row
 * maps; binary, fixed-length, and INT96 leaves become {@code ByteBuffer}.
 *
 * <p>Construction uses {@code LocalInputFile} with no Hadoop {@code Path}, {@code FileSystem}, or {@code Configuration}
 * to keep the reader off the Hadoop user-group path that aborts under recent JDKs.
 */
final class ParquetJavaOracle {

    private ParquetJavaOracle() {}

    static List<Map<String, Object>> read(Path fixture) {
        List<Map<String, Object>> rows = new ArrayList<>();
        try (ParquetReader<Group> reader = new ParquetReader.Builder<Group>(new LocalInputFile(fixture)) {
            @Override
            protected ReadSupport<Group> getReadSupport() {
                return new GroupReadSupport();
            }
        }.build()) {
            Group group = reader.read();
            while (group != null) {
                rows.add(canonicalize(group));
                group = reader.read();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Oracle failed to read " + fixture, e);
        }
        return rows;
    }

    static Map<String, Object> canonicalize(Group group) {
        GroupType type = group.getType();
        Map<String, Object> row = LinkedHashMap.newLinkedHashMap(type.getFieldCount());
        for (int field = 0; field < type.getFieldCount(); field++) {
            row.put(type.getFieldName(field), canonicalizeField(group, field));
        }
        return row;
    }

    private static Object canonicalizeField(Group group, int field) {
        Type fieldType = group.getType().getType(field);
        if (isList(fieldType)) {
            return canonicalizeListField(group, field, fieldType.asGroupType());
        }
        if (isMap(fieldType)) {
            return canonicalizeMapField(group, field, fieldType.asGroupType());
        }
        if (fieldType.isRepetition(Type.Repetition.REPEATED)) {
            return canonicalizeRepeated(group, field, fieldType);
        }
        if (group.getFieldRepetitionCount(field) == 0) {
            return null;
        }
        return canonicalizeMember(group, field, 0, fieldType);
    }

    // A standard LIST is an optional/required group holding one repeated child; the element is that child's lone field
    // when the child is a group (three-level), or the repeated primitive itself (two-level).
    private static Object canonicalizeListField(Group group, int field, GroupType listType) {
        if (group.getFieldRepetitionCount(field) == 0) {
            return null;
        }
        Group listGroup = group.getGroup(field, 0);
        Type repeated = listType.getType(0);
        int count = listGroup.getFieldRepetitionCount(0);
        List<Object> elements = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            if (repeated.isPrimitive()) {
                elements.add(canonicalizeMember(listGroup, 0, i, repeated));
            } else {
                elements.add(threeLevelElement(listGroup.getGroup(0, i), repeated.asGroupType()));
            }
        }
        return elements;
    }

    // In a three-level list the element sits inside a repeated wrapper group. When that element is optional and absent,
    // the wrapper holds zero repetitions of it, which canonicalizes to a null element rather than a value lookup.
    private static Object threeLevelElement(Group wrapper, GroupType wrapperType) {
        if (wrapper.getFieldRepetitionCount(0) == 0) {
            return null;
        }
        return canonicalizeMember(wrapper, 0, 0, wrapperType.getType(0));
    }

    private static Object canonicalizeMapField(Group group, int field, GroupType mapType) {
        if (group.getFieldRepetitionCount(field) == 0) {
            return null;
        }
        Group mapGroup = group.getGroup(field, 0);
        GroupType keyValueType = mapType.getType(0).asGroupType();
        Type keyType = keyValueType.getType(0);
        Type valueType = keyValueType.getType(1);
        int count = mapGroup.getFieldRepetitionCount(0);
        Map<Object, Object> entries = LinkedHashMap.newLinkedHashMap(count);
        for (int i = 0; i < count; i++) {
            Group entry = mapGroup.getGroup(0, i);
            Object key = canonicalizeMember(entry, 0, 0, keyType);
            Object value = entry.getFieldRepetitionCount(1) == 0 ? null : canonicalizeMember(entry, 1, 0, valueType);
            entries.put(key, value);
        }
        return entries;
    }

    // Legacy repeated field with no LIST annotation: each repetition is an element; a repeated group becomes a struct
    // row, a repeated primitive a scalar.
    private static Object canonicalizeRepeated(Group group, int field, Type fieldType) {
        int count = group.getFieldRepetitionCount(field);
        List<Object> elements = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            elements.add(canonicalizeMember(group, field, i, fieldType));
        }
        return elements;
    }

    private static Object canonicalizeMember(Group group, int field, int index, Type memberType) {
        if (memberType.isPrimitive()) {
            return primitiveValue(
                    group, field, index, memberType.asPrimitiveType().getPrimitiveTypeName());
        }
        return canonicalize(group.getGroup(field, index));
    }

    private static Object primitiveValue(Group group, int field, int index, PrimitiveType.PrimitiveTypeName kind) {
        return switch (kind) {
            case BOOLEAN -> group.getBoolean(field, index);
            case INT32 -> group.getInteger(field, index);
            case INT64 -> group.getLong(field, index);
            case FLOAT -> group.getFloat(field, index);
            case DOUBLE -> group.getDouble(field, index);
            case BINARY, FIXED_LEN_BYTE_ARRAY -> toByteBuffer(group.getBinary(field, index));
            case INT96 -> toByteBuffer(group.getInt96(field, index));
        };
    }

    private static ByteBuffer toByteBuffer(Binary binary) {
        return ByteBuffer.wrap(binary.getBytes());
    }

    private static boolean isList(Type type) {
        return !type.isPrimitive()
                && type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation;
    }

    private static boolean isMap(Type type) {
        return !type.isPrimitive()
                && type.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation;
    }
}
