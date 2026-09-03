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
package io.tileverse.parquetry.internal.write.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Round trips for containers authored DIRECTLY inside a list element or map entry value:
 * {@link ParquetRecordBatchBuilder#addList()} / {@link ParquetRecordBatchBuilder#addMap()} open the inner container as
 * the current element (list scope) or as the current entry's value (map scope). Covers the per-depth truth table: null
 * outer, empty outer, null inner, empty inner, populated.
 */
class NestedContainerAuthoringRoundTripIT {

    @TempDir
    Path tempDir;

    private static final ColumnPath OUTER = ColumnPath.of("outer");

    /**
     * Schema: {@code optional group outer (LIST) { repeated group list { optional group element (LIST) { repeated group
     * list { optional int32 element; } } } }}.
     *
     * <p>Row 0: [[1,2],[3]]. Row 1: [[], null, [4]]. Row 2: []. Row 3: null.
     */
    @Test
    void listOfListOfIntRoundTrip() throws IOException {
        ParquetSchema schema = rootOf(listNode("outer", listNode("element", optionalInt("element"))));
        Path out = tempDir.resolve("list_of_list.parquet");

        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(out), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            appender.beginList("outer")
                    .addList()
                    .addInt(1)
                    .addInt(2)
                    .endList()
                    .addList()
                    .addInt(3)
                    .endList()
                    .endList()
                    .endRow();
            appender.beginList("outer")
                    .addList()
                    .endList()
                    .addNull()
                    .addList()
                    .addInt(4)
                    .endList()
                    .endList()
                    .endRow();
            appender.beginList("outer").endList().endRow();
            appender.setNull(OUTER).endRow();
        }

        List<ParquetRecord> rows = WriteConformanceSupport.readAll(out);
        assertThat(rows).hasSize(4);

        assertThat(rows.get(0).get(OUTER)).isEqualTo(List.of(List.of(1, 2), List.of(3)));
        List<?> row1 = asList(rows.get(1).get(OUTER));
        assertThat(row1).hasSize(3);
        assertThat(asList(row1.get(0))).as("empty inner list").isEmpty();
        assertThat(row1.get(1)).as("null inner list").isNull();
        assertThat(row1.get(2)).isEqualTo(List.of(4));
        assertThat(asList(rows.get(2).get(OUTER))).as("empty outer list").isEmpty();
        assertThat(rows.get(3).get(OUTER)).as("null outer list").isNull();
    }

    private static final ColumnPath KV_KEY = ColumnPath.of("key_value", "key");
    private static final ColumnPath KV_VALUE = ColumnPath.of("key_value", "value");
    private static final ColumnPath M = ColumnPath.of("m");

    /**
     * The Overture divisions {@code hierarchies} shape: {@code list<list<struct{division_id, subtype}>>}. Inner struct
     * elements are authored with {@link ParquetRecordBatchBuilder#addElement()} and relative paths inside the
     * {@code addList()} scope; one inner element is a null struct.
     */
    @Test
    void listOfListOfStructRoundTrip() throws IOException {
        SchemaNode.Group structElement = new SchemaNode.Group(
                "element",
                Repetition.OPTIONAL,
                List.of(optionalString("division_id"), optionalString("subtype")),
                Optional.empty(),
                -1);
        ParquetSchema schema = rootOf(listNode("hierarchies", listNode("element", structElement)));
        Path out = tempDir.resolve("list_of_list_of_struct.parquet");
        ColumnPath hierarchies = ColumnPath.of("hierarchies");
        ColumnPath divisionId = ColumnPath.of("element", "division_id");
        ColumnPath subtype = ColumnPath.of("element", "subtype");

        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(out), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            appender.beginList("hierarchies")
                    .addList()
                    .addElement()
                    .setString(divisionId, "d1")
                    .setString(subtype, "country")
                    .endElement()
                    .addElement()
                    .setString(divisionId, "d2")
                    .setNull(subtype)
                    .endElement()
                    .endList()
                    .addList()
                    .addNull()
                    .addElement()
                    .setString(divisionId, "d3")
                    .setString(subtype, "region")
                    .endElement()
                    .endList()
                    .endList()
                    .endRow();
        }

        List<ParquetRecord> rows = WriteConformanceSupport.readAll(out);
        assertThat(rows).hasSize(1);

        List<?> outer = asList(rows.get(0).get(hierarchies));
        assertThat(outer).hasSize(2);

        List<?> first = asList(outer.get(0));
        assertThat(first).hasSize(2);
        ParquetRecord d1 = (ParquetRecord) first.get(0);
        assertThat(asString(d1.get(ColumnPath.of("division_id")))).isEqualTo("d1");
        assertThat(asString(d1.get(ColumnPath.of("subtype")))).isEqualTo("country");
        ParquetRecord d2 = (ParquetRecord) first.get(1);
        assertThat(asString(d2.get(ColumnPath.of("division_id")))).isEqualTo("d2");
        assertThat(d2.get(ColumnPath.of("subtype"))).as("null struct field").isNull();

        List<?> second = asList(outer.get(1));
        assertThat(second).hasSize(2);
        assertThat(second.get(0)).as("null struct element").isNull();
        ParquetRecord d3 = (ParquetRecord) second.get(1);
        assertThat(asString(d3.get(ColumnPath.of("division_id")))).isEqualTo("d3");
    }

    /**
     * Schema: {@code map<string, list<int>>}. Entry values: populated list, empty list, and an entry whose value is
     * left unset (a null value list).
     */
    @Test
    void mapOfListRoundTrip() throws IOException {
        ParquetSchema schema = rootOf(mapNode("m", requiredStringKey(), listNode("value", optionalInt("element"))));
        Path out = tempDir.resolve("map_of_list.parquet");

        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(out), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            appender.beginMap("m")
                    .putEntry()
                    .setString(KV_KEY, "a")
                    .addList()
                    .addInt(1)
                    .addInt(2)
                    .endList()
                    .endEntry()
                    .putEntry()
                    .setString(KV_KEY, "b")
                    .addList()
                    .endList()
                    .endEntry()
                    .putEntry()
                    .setString(KV_KEY, "c")
                    .endEntry()
                    .endMap()
                    .endRow();
            appender.beginMap("m").endMap().endRow();
            appender.setNull(M).endRow();
        }

        List<ParquetRecord> rows = WriteConformanceSupport.readAll(out);
        assertThat(rows).hasSize(3);

        java.util.Map<String, Object> row0 = keyedMap(rows.get(0).get(M));
        assertThat(row0).hasSize(3);
        assertThat(row0.get("a")).isEqualTo(List.of(1, 2));
        assertThat(asList(row0.get("b"))).as("empty value list").isEmpty();
        assertThat(row0.get("c")).as("unset value list reads as null").isNull();
        assertThat((java.util.Map<?, ?>) rows.get(1).get(M)).isEmpty();
        assertThat(rows.get(2).get(M)).isNull();
    }

    /** Schema: {@code list<map<string, int>>}. Elements: populated map, empty map, null map. */
    @Test
    void listOfMapRoundTrip() throws IOException {
        ParquetSchema schema = rootOf(listNode("outer", mapNode("element", requiredStringKey(), optionalInt("value"))));
        Path out = tempDir.resolve("list_of_map.parquet");

        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(out), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            appender.beginList("outer")
                    .addMap()
                    .putEntry()
                    .setString(KV_KEY, "a")
                    .setInt(KV_VALUE, 1)
                    .endEntry()
                    .endMap()
                    .addMap()
                    .endMap()
                    .addNull()
                    .endList()
                    .endRow();
        }

        List<ParquetRecord> rows = WriteConformanceSupport.readAll(out);
        List<?> elements = asList(rows.get(0).get(OUTER));
        assertThat(elements).hasSize(3);
        java.util.Map<String, Object> first = keyedMap(elements.get(0));
        assertThat(first).hasSize(1);
        assertThat(first.get("a")).isEqualTo(1);
        assertThat((java.util.Map<?, ?>) elements.get(1))
                .as("empty map element")
                .isEmpty();
        assertThat(elements.get(2)).as("null map element").isNull();
    }

    /** Schema: {@code map<string, map<string, int>>}. Values: populated inner map, empty inner map, unset (null). */
    @Test
    void mapOfMapRoundTrip() throws IOException {
        ParquetSchema schema =
                rootOf(mapNode("m", requiredStringKey(), mapNode("value", requiredStringKey(), optionalInt("value"))));
        Path out = tempDir.resolve("map_of_map.parquet");

        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(out), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            appender.beginMap("m")
                    .putEntry()
                    .setString(KV_KEY, "out")
                    .addMap()
                    .putEntry()
                    .setString(KV_KEY, "in")
                    .setInt(KV_VALUE, 5)
                    .endEntry()
                    .endMap()
                    .endEntry()
                    .putEntry()
                    .setString(KV_KEY, "empty")
                    .addMap()
                    .endMap()
                    .endEntry()
                    .putEntry()
                    .setString(KV_KEY, "nil")
                    .endEntry()
                    .endMap()
                    .endRow();
        }

        List<ParquetRecord> rows = WriteConformanceSupport.readAll(out);
        java.util.Map<String, Object> row0 = keyedMap(rows.get(0).get(M));
        assertThat(row0).hasSize(3);
        java.util.Map<String, Object> inner = keyedMap(row0.get("out"));
        assertThat(inner).hasSize(1);
        assertThat(inner.get("in")).isEqualTo(5);
        assertThat((java.util.Map<?, ?>) row0.get("empty")).isEmpty();
        assertThat(row0.get("nil")).isNull();
    }

    /**
     * Struct-MEDIATED inner container: {@code list<struct{name, tags: list<int>}>}. The tags list is opened by its
     * relative path from the element wrapper inside an open {@link ParquetRecordBatchBuilder#addElement()} scope; the
     * element itself is still closed by {@link ParquetRecordBatchBuilder#endElement()}.
     */
    @Test
    void listOfStructWithNestedListRoundTrip() throws IOException {
        SchemaNode.Group tags = listNode("tags", optionalInt("element"));
        SchemaNode.Group structElement = new SchemaNode.Group(
                "element", Repetition.OPTIONAL, List.of(optionalString("name"), tags), Optional.empty(), -1);
        ParquetSchema schema = rootOf(listNode("features", structElement));
        Path out = tempDir.resolve("list_of_struct_with_list.parquet");
        ColumnPath features = ColumnPath.of("features");
        ColumnPath name = ColumnPath.of("element", "name");
        ColumnPath elementTags = ColumnPath.of("element", "tags");

        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(out), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            appender.beginList("features")
                    .addElement()
                    .setString(name, "f1")
                    .beginList(elementTags)
                    .addInt(7)
                    .addInt(8)
                    .endList()
                    .endElement()
                    .addElement()
                    .setString(name, "f2")
                    .endElement()
                    .endList()
                    .endRow();
        }

        List<ParquetRecord> rows = WriteConformanceSupport.readAll(out);
        List<?> elements = asList(rows.get(0).get(features));
        assertThat(elements).hasSize(2);
        ParquetRecord f1 = (ParquetRecord) elements.get(0);
        assertThat(asString(f1.get(ColumnPath.of("name")))).isEqualTo("f1");
        assertThat(f1.get(ColumnPath.of("tags"))).isEqualTo(List.of(7, 8));
        ParquetRecord f2 = (ParquetRecord) elements.get(1);
        assertThat(asString(f2.get(ColumnPath.of("name")))).isEqualTo("f2");
        assertThat(f2.get(ColumnPath.of("tags")))
                .as("unset tags list reads as null")
                .isNull();
    }

    /** Schema: {@code list<list<list<int>>>}. Middle elements: populated, empty, null. */
    @Test
    void threeDeepListRoundTrip() throws IOException {
        ParquetSchema schema =
                rootOf(listNode("deep", listNode("element", listNode("element", optionalInt("element")))));
        Path out = tempDir.resolve("three_deep_list.parquet");
        ColumnPath deep = ColumnPath.of("deep");

        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(out), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            appender.beginList("deep")
                    .addList()
                    .addList()
                    .addInt(1)
                    .endList()
                    .addList()
                    .addInt(2)
                    .addInt(3)
                    .endList()
                    .endList()
                    .addList()
                    .endList()
                    .addNull()
                    .endList()
                    .endRow();
        }

        List<ParquetRecord> rows = WriteConformanceSupport.readAll(out);
        List<?> outer = asList(rows.get(0).get(deep));
        assertThat(outer).hasSize(3);
        assertThat(outer.get(0)).isEqualTo(List.of(List.of(1), List.of(2, 3)));
        assertThat(asList(outer.get(1))).as("empty middle list").isEmpty();
        assertThat(outer.get(2)).as("null middle list").isNull();
    }

    // --- schema helpers ---

    /** Builds {@code <name> (LIST, OPTIONAL) { repeated group list { <element> } }}. */
    private static SchemaNode.Group listNode(String name, SchemaNode element) {
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
    }

    private static SchemaNode.Primitive optionalInt(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static ParquetSchema rootOf(SchemaNode field) {
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(field), Optional.empty(), -1));
    }

    /** Builds {@code <name> (MAP, OPTIONAL) { repeated group key_value { <key>; <value>; } }}. */
    private static SchemaNode.Group mapNode(String name, SchemaNode key, SchemaNode value) {
        SchemaNode.Group keyValue =
                new SchemaNode.Group("key_value", Repetition.REPEATED, List.of(key, value), Optional.empty(), -1);
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, List.of(keyValue), Optional.of(new LogicalType.MapType()), -1);
    }

    private static SchemaNode.Primitive requiredStringKey() {
        return new SchemaNode.Primitive(
                "key",
                Repetition.REQUIRED,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
    }

    private static SchemaNode.Primitive optionalString(String name) {
        return new SchemaNode.Primitive(
                name,
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
    }

    private static List<?> asList(Object value) {
        assertThat(value).isInstanceOf(List.class);
        return (List<?>) value;
    }

    /** Reads a map cell back with its binary keys decoded to strings, values kept as read. */
    private static java.util.Map<String, Object> keyedMap(Object value) {
        java.util.Map<String, Object> out = new java.util.LinkedHashMap<>();
        for (java.util.Map.Entry<?, ?> entry : ((java.util.Map<?, ?>) value).entrySet()) {
            out.put(asString(entry.getKey()), entry.getValue());
        }
        return out;
    }

    private static String asString(Object value) {
        java.lang.foreign.MemorySegment segment = (java.lang.foreign.MemorySegment) value;
        return new String(
                segment.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE), java.nio.charset.StandardCharsets.UTF_8);
    }
}
