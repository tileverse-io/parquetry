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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class IcebergNestedSchemaTest {

    @Test
    void aFlatSchemaIsEmpty() {
        IcebergNestedSchema nested = parse("""
                [ {"id": 1, "name": "id", "required": false, "type": "long"} ]
                """);

        assertThat(nested.isEmpty()).isTrue();
    }

    @Test
    void capturesAStructColumnAndItsMembers() {
        IcebergNestedSchema nested = parse("""
                [ {"id": 1, "name": "id", "required": false, "type": "string"},
                  {"id": 2, "name": "bbox", "required": false,
                   "type": {"type": "struct", "fields": [
                     {"id": 5, "name": "xmin", "required": false, "type": "double"},
                     {"id": 6, "name": "ymin", "required": false, "type": "double"}
                   ]}} ]
                """);

        assertThat(nested.isEmpty()).isFalse();
        assertThat(nested.parentOf(2)).isEqualTo(0);
        assertThat(nested.nameOf(2)).isEqualTo("bbox");
        assertThat(nested.parentOf(5)).isEqualTo(2);
        assertThat(nested.nameOf(5)).isEqualTo("xmin");
        assertThat(nested.idAt(2, "xmin")).isEqualTo(5);
        assertThat(nested.path(5)).isEqualTo("bbox.xmin");
        assertThat(nested.parentOf(1)).isEqualTo(IcebergNestedSchema.ABSENT);
    }

    @Test
    void capturesListElementAndMapKeyValue() {
        IcebergNestedSchema nested = parse("""
                [ {"id": 3, "name": "tags", "required": false,
                   "type": {"type": "list", "element-id": 9, "element-required": false,
                            "element": "string"}},
                  {"id": 4, "name": "props", "required": false,
                   "type": {"type": "map", "key-id": 10, "key": "string",
                            "value-id": 11, "value-required": false, "value": "long"}} ]
                """);

        assertThat(nested.parentOf(9)).isEqualTo(3);
        assertThat(nested.nameOf(9)).isEqualTo("element");
        assertThat(nested.parentOf(10)).isEqualTo(4);
        assertThat(nested.nameOf(10)).isEqualTo("key");
        assertThat(nested.parentOf(11)).isEqualTo(4);
        assertThat(nested.nameOf(11)).isEqualTo("value");
        assertThat(nested.path(9)).isEqualTo("tags.element");
    }

    @Test
    void recursesIntoNestedStructWithinAStruct() {
        IcebergNestedSchema nested = parse("""
                [ {"id": 2, "name": "outer", "required": false,
                   "type": {"type": "struct", "fields": [
                     {"id": 5, "name": "inner", "required": false,
                      "type": {"type": "struct", "fields": [
                        {"id": 6, "name": "leaf", "required": false, "type": "long"}
                      ]}}
                   ]}} ]
                """);

        assertThat(nested.parentOf(6)).isEqualTo(5);
        assertThat(nested.path(6)).isEqualTo("outer.inner.leaf");
    }

    @Test
    void sameMemberNameUnderDifferentParentsMapsToDistinctIds() {
        IcebergNestedSchema nested = parse("""
                [ {"id": 2, "name": "a", "required": false,
                   "type": {"type": "struct", "fields": [
                     {"id": 5, "name": "x", "required": false, "type": "double"}]}},
                  {"id": 3, "name": "b", "required": false,
                   "type": {"type": "struct", "fields": [
                     {"id": 6, "name": "x", "required": false, "type": "double"}]}} ]
                """);

        assertThat(nested.idAt(2, "x")).isEqualTo(5);
        assertThat(nested.idAt(3, "x")).isEqualTo(6);
        assertThat(nested.path(5)).isEqualTo("a.x");
        assertThat(nested.path(6)).isEqualTo("b.x");
    }

    @Test
    void capturesAListNestedInsideAStruct() {
        IcebergNestedSchema nested = parse("""
                [ {"id": 2, "name": "outer", "required": false,
                   "type": {"type": "struct", "fields": [
                     {"id": 5, "name": "tags", "required": false,
                      "type": {"type": "list", "element-id": 9, "element-required": false,
                               "element": "string"}}]}} ]
                """);

        assertThat(nested.parentOf(9)).isEqualTo(5);
        assertThat(nested.path(9)).isEqualTo("outer.tags.element");
    }

    @Test
    void absentLookupsReturnSentinels() {
        IcebergNestedSchema nested = IcebergNestedSchema.of(null);

        assertThat(nested.isEmpty()).isTrue();
        assertThat(nested.parentOf(99)).isEqualTo(IcebergNestedSchema.ABSENT);
        assertThat(nested.nameOf(99)).isNull();
        assertThat(nested.idAt(1, "missing")).isEqualTo(IcebergNestedSchema.ABSENT);
        assertThat(nested.path(99)).isNull();
        assertThat(nested.hasId(99)).isFalse();
        assertThat(nested.hasSlot(1, "missing")).isFalse();
    }

    @Test
    void aStructMemberMissingItsIdFailsLoud() {
        assertThatThrownBy(() -> parse("""
                [ {"id": 2, "name": "bbox", "required": false,
                   "type": {"type": "struct", "fields": [
                     {"name": "xmin", "required": false, "type": "double"}]}} ]
                """))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("nested schema");
    }

    private static IcebergNestedSchema parse(String fieldsJson) {
        JsonNode fields = JsonMapper.shared().readTree(fieldsJson);
        return IcebergNestedSchema.of(fields);
    }
}
