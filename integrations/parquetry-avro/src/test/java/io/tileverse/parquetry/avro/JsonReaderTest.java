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
package io.tileverse.parquetry.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class JsonReaderTest {

    @Test
    @SuppressWarnings("unchecked")
    void parsesNestedObjectPreservingFieldOrder() {
        Object tree =
                JsonReader.parse("{\"type\":\"record\",\"name\":\"r\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}");
        Map<String, Object> root = (Map<String, Object>) tree;
        assertThat(root).containsEntry("type", "record").containsEntry("name", "r");
        List<Object> fields = (List<Object>) root.get("fields");
        Map<String, Object> first = (Map<String, Object>) fields.get(0);
        assertThat(first).containsEntry("name", "a").containsEntry("type", "int");
    }

    @Test
    void parsesScalarTypes() {
        assertThat(JsonReader.parse("\"hello\"")).isEqualTo("hello");
        assertThat(JsonReader.parse("42")).isEqualTo(42L);
        assertThat(JsonReader.parse("3.5")).isEqualTo(3.5d);
        assertThat(JsonReader.parse("true")).isEqualTo(Boolean.TRUE);
        assertThat(JsonReader.parse("null")).isNull();
    }

    @Test
    @SuppressWarnings("unchecked")
    void preservesIntegerFieldIdAsLong() {
        Map<String, Object> root = (Map<String, Object>) JsonReader.parse("{\"field-id\":2}");
        assertThat(root.get("field-id")).isEqualTo(2L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void parsesArraysAndMixedScalars() {
        Map<String, Object> root = (Map<String, Object>)
                JsonReader.parse("{\"symbols\":[\"A\",\"B\"],\"size\":16,\"flag\":false,\"ratio\":1.25,\"none\":null}");
        assertThat((List<Object>) root.get("symbols")).containsExactly("A", "B");
        assertThat(root.get("size")).isEqualTo(16L);
        assertThat(root.get("flag")).isEqualTo(Boolean.FALSE);
        assertThat(root.get("ratio")).isEqualTo(1.25d);
        assertThat(root).containsKey("none");
        assertThat(root.get("none")).isNull();
    }

    @Test
    void malformedJsonThrowsAvroFormatException() {
        assertThatThrownBy(() -> JsonReader.parse("{\"a\":")).isInstanceOf(AvroFormatException.class);
    }

    @Test
    void emptyInputThrowsAvroFormatException() {
        assertThatThrownBy(() -> JsonReader.parse("")).isInstanceOf(AvroFormatException.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    void parsesEmptyObjectAndArray() {
        Map<String, Object> root = (Map<String, Object>) JsonReader.parse("{\"a\":{},\"b\":[]}");
        assertThat((Map<String, Object>) root.get("a")).isEmpty();
        assertThat((List<Object>) root.get("b")).isEmpty();
    }
}
