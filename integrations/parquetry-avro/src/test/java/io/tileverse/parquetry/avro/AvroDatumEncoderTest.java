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
package io.tileverse.parquetry.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class AvroDatumEncoderTest {

    private final AvroDatumEncoder encoder = new AvroDatumEncoder();
    private final AvroDatumDecoder decoder = new AvroDatumDecoder();

    private Object roundTrip(AvroSchema schema, Object value) {
        AvroBinaryEncoder out = new AvroBinaryEncoder();
        encoder.encode(schema, value, out);
        return decoder.decode(schema, new AvroBinaryDecoder(MemorySegment.ofArray(out.toByteArray())));
    }

    @Test
    void encodesRecordFromMapAndFillsDefaults() {
        AvroSchema schema = AvroSchema.parse("""
                {"type":"record","name":"R","fields":[
                  {"name":"a","type":"int"},
                  {"name":"b","type":"string","default":"x"}]}
                """);
        AvroRecord decoded = (AvroRecord) roundTrip(schema, Map.of("a", 5));
        assertThat(decoded.get("a")).isEqualTo(5);
        assertThat(decoded.get("b")).isEqualTo("x");
    }

    @Test
    void encodesRecordFromAvroRecord() {
        AvroSchema schema = AvroSchema.parse("""
                {"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}
                """);
        AvroRecord input = (AvroRecord) roundTrip(schema, Map.of("a", 9));
        AvroRecord again = (AvroRecord) roundTrip(schema, input);
        assertThat(again.get("a")).isEqualTo(9);
    }

    @Test
    void rejectsUnknownMapKey() {
        AvroSchema schema = AvroSchema.parse("""
                {"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}
                """);
        assertThatThrownBy(() -> roundTrip(schema, Map.of("a", 1, "typo", 2)))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("typo");
    }

    @Test
    void rejectsMissingFieldWithoutDefault() {
        AvroSchema schema = AvroSchema.parse("""
                {"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}
                """);
        assertThatThrownBy(() -> roundTrip(schema, Map.of()))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("a");
    }

    @Test
    void selectsUnionBranchByRuntimeType() {
        AvroSchema schema = AvroSchema.parse("[\"null\",\"string\"]");
        assertThat(roundTrip(schema, null)).isNull();
        assertThat(roundTrip(schema, "hi")).isEqualTo("hi");
    }

    @Test
    void encodesArrayMapEnumAndFixed() {
        AvroSchema array = AvroSchema.parse("{\"type\":\"array\",\"items\":\"int\"}");
        assertThat(roundTrip(array, List.of(1, 2, 3))).isEqualTo(List.of(1, 2, 3));

        AvroSchema map = AvroSchema.parse("{\"type\":\"map\",\"values\":\"long\"}");
        assertThat(roundTrip(map, Map.of("k", 4L))).isEqualTo(Map.of("k", 4L));

        AvroSchema anEnum = AvroSchema.parse("{\"type\":\"enum\",\"name\":\"E\",\"symbols\":[\"A\",\"B\"]}");
        assertThat(roundTrip(anEnum, "B")).isEqualTo("B");

        AvroSchema fixed = AvroSchema.parse("{\"type\":\"fixed\",\"name\":\"F\",\"size\":3}");
        Object decoded = roundTrip(fixed, new byte[] {7, 8, 9});
        assertThat(((MemorySegment) decoded).toArray(java.lang.foreign.ValueLayout.JAVA_BYTE))
                .containsExactly(7, 8, 9);
    }

    @Test
    void encodesNestedRecord() {
        AvroSchema schema = AvroSchema.parse("""
                {"type":"record","name":"Outer","fields":[
                  {"name":"inner","type":{"type":"record","name":"Inner","fields":[
                    {"name":"x","type":"int"}]}}]}
                """);
        AvroRecord outer = (AvroRecord) roundTrip(schema, Map.of("inner", Map.of("x", 42)));
        AvroRecord inner = (AvroRecord) outer.get("inner");
        assertThat(inner.get("x")).isEqualTo(42);
    }

    @Test
    void encodesArrayOfRecords() {
        AvroSchema schema = AvroSchema.parse("""
                {"type":"array","items":{"type":"record","name":"Point","fields":[
                  {"name":"x","type":"int"}]}}
                """);
        @SuppressWarnings("unchecked")
        List<Object> decoded = (List<Object>) roundTrip(schema, List.of(Map.of("x", 1), Map.of("x", 2)));
        assertThat(decoded).hasSize(2);
        assertThat(((AvroRecord) decoded.get(0)).get("x")).isEqualTo(1);
        assertThat(((AvroRecord) decoded.get(1)).get("x")).isEqualTo(2);
    }

    @Test
    void encodesMapOfRecords() {
        AvroSchema schema = AvroSchema.parse("""
                {"type":"map","values":{"type":"record","name":"Cell","fields":[
                  {"name":"v","type":"int"}]}}
                """);
        @SuppressWarnings("unchecked")
        Map<String, Object> decoded = (Map<String, Object>) roundTrip(schema, Map.of("k", Map.of("v", 7)));
        assertThat(((AvroRecord) decoded.get("k")).get("v")).isEqualTo(7);
    }

    @Test
    void encodesUnionInsideArray() {
        AvroSchema schema = AvroSchema.parse("{\"type\":\"array\",\"items\":[\"null\",\"string\"]}");
        List<String> input = Arrays.asList("hi", null);
        assertThat(roundTrip(schema, input)).isEqualTo(input);
    }

    @Test
    void encodesRecursiveSchemaViaNamedReference() {
        AvroSchema schema = AvroSchema.parse("""
                {"type":"record","name":"Node","fields":[
                  {"name":"value","type":"int"},
                  {"name":"next","type":["null","Node"]}]}
                """);
        Map<String, Object> tail = new HashMap<>();
        tail.put("value", 2);
        tail.put("next", null);
        Map<String, Object> chain = new HashMap<>();
        chain.put("value", 1);
        chain.put("next", tail);
        AvroRecord head = (AvroRecord) roundTrip(schema, chain);
        assertThat(head.get("value")).isEqualTo(1);
        AvroRecord second = (AvroRecord) head.get("next");
        assertThat(second.get("value")).isEqualTo(2);
        assertThat(second.get("next")).isNull();
    }

    @Test
    void encodesEmptyArrayAndEmptyMap() {
        AvroSchema array = AvroSchema.parse("{\"type\":\"array\",\"items\":\"int\"}");
        assertThat(roundTrip(array, List.of())).isEqualTo(List.of());

        AvroSchema map = AvroSchema.parse("{\"type\":\"map\",\"values\":\"long\"}");
        assertThat(roundTrip(map, Map.of())).isEqualTo(Map.of());
    }
}
