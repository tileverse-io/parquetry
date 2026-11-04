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
package io.tileverse.parquetry.schema;

import static io.tileverse.parquetry.schema.Repetition.OPTIONAL;
import static io.tileverse.parquetry.schema.Repetition.REPEATED;
import static io.tileverse.parquetry.schema.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.LogicalType;

class StandardListsTest {

    private static final int NO_FIELD_ID = -1;

    @Test
    void repeatedPrimitiveIsAList() {
        ParquetSchema stored = schema(int32("Int32_list", REPEATED));

        ParquetSchema expected = schema(standardList("Int32_list", REQUIRED, int32("element", REQUIRED)));
        assertThat(StandardLists.of(stored)).isEqualTo(expected);
    }

    @Test
    void repeatedGroupIsTheElement() {
        SchemaNode.Group phone = group("phone", REPEATED, int64("number", REQUIRED), string("kind", OPTIONAL));
        ParquetSchema stored = schema(group("phoneNumbers", OPTIONAL, phone));

        SchemaNode.Group element = group("element", REQUIRED, int64("number", REQUIRED), string("kind", OPTIONAL));
        ParquetSchema expected = schema(group("phoneNumbers", OPTIONAL, standardList("phone", REQUIRED, element)));
        assertThat(StandardLists.of(stored)).isEqualTo(expected);
    }

    // A one-field group stays a struct of that field, as the format reads it.
    @Test
    void oneFieldGroupStaysAStruct() {
        ParquetSchema stored = schema(group("phone", REPEATED, int64("number", REQUIRED)));

        SchemaNode.Group element = group("element", REQUIRED, int64("number", REQUIRED));
        assertThat(StandardLists.of(stored)).isEqualTo(schema(standardList("phone", REQUIRED, element)));
    }

    @Test
    void twoLevelListOfValues() {
        ParquetSchema stored = schema(list("nums", OPTIONAL, int32("array", REPEATED)));

        ParquetSchema expected = schema(standardList("nums", OPTIONAL, int32("element", REQUIRED)));
        assertThat(StandardLists.of(stored)).isEqualTo(expected);
    }

    @Test
    void twoLevelListOfStructs() {
        SchemaNode.Group repeated = group("element", REPEATED, string("str", REQUIRED), int32("num", REQUIRED));
        ParquetSchema stored = schema(list("my_list", OPTIONAL, repeated));

        SchemaNode.Group element = group("element", REQUIRED, string("str", REQUIRED), int32("num", REQUIRED));
        assertThat(StandardLists.of(stored)).isEqualTo(schema(standardList("my_list", OPTIONAL, element)));
    }

    // The shape of old_list_structure.parquet: a list of lists of int.
    @Test
    void listOfTwoLevelLists() {
        SchemaNode.Group inner = list("array", REPEATED, int32("array", REPEATED));
        ParquetSchema stored = schema(list("a", REQUIRED, inner));

        SchemaNode.Group element = standardList("element", REQUIRED, int32("element", REQUIRED));
        assertThat(StandardLists.of(stored)).isEqualTo(schema(standardList("a", REQUIRED, element)));
    }

    @ParameterizedTest
    @MethodSource("oneTupleNames")
    void oneTupleIsAStruct(String repeatedName) {
        ParquetSchema stored =
                schema(list("my_list", OPTIONAL, group(repeatedName, REPEATED, string("str", REQUIRED))));

        SchemaNode.Group element = group("element", REQUIRED, string("str", REQUIRED));
        assertThat(StandardLists.of(stored)).isEqualTo(schema(standardList("my_list", OPTIONAL, element)));
    }

    static Stream<String> oneTupleNames() {
        return Stream.of("array", "my_list_tuple");
    }

    // The shape of parquet-thrift for list<list<struct{x}>>: the inner one-tuple is named after the stored list.
    @Test
    void nestedOneTupleIsAStruct() {
        SchemaNode.Group innerTuple = group("a_tuple_tuple", REPEATED, int32("x", REQUIRED));
        ParquetSchema stored = schema(list("a", REQUIRED, list("a_tuple", REPEATED, innerTuple)));

        SchemaNode.Group struct = group("element", REQUIRED, int32("x", REQUIRED));
        SchemaNode.Group element = standardList("element", REQUIRED, struct);
        assertThat(StandardLists.of(stored)).isEqualTo(schema(standardList("a", REQUIRED, element)));
    }

    // A group of one repeated field is the element, also without a list annotation: a struct holding a list.
    @Test
    void groupOfAListIsTheElement() {
        ParquetSchema stored = schema(list("pairs", OPTIONAL, group("pair", REPEATED, int32("x", REPEATED))));

        SchemaNode.Group element = group("element", REQUIRED, standardList("x", REQUIRED, int32("element", REQUIRED)));
        assertThat(StandardLists.of(stored)).isEqualTo(schema(standardList("pairs", OPTIONAL, element)));
    }

    // A three-level list keeps the names written by its writer.
    @Test
    void threeLevelListUnchanged() {
        ParquetSchema stored = schema(list("hive", OPTIONAL, group("bag", REPEATED, int32("array_element", OPTIONAL))));

        assertThat(StandardLists.of(stored)).isSameAs(stored);
    }

    @Test
    void mapEntriesUnchanged() {
        SchemaNode.Group keyValue = group("key_value", REPEATED, string("key", REQUIRED), int32("value", OPTIONAL));
        ParquetSchema stored = schema(annotated("m", OPTIONAL, new LogicalType.MapType(), keyValue));

        assertThat(StandardLists.of(stored)).isSameAs(stored);
    }

    @Test
    void repeatedFieldInAMapValue() {
        SchemaNode.Group storedValue = group("value", OPTIONAL, int32("codes", REPEATED));
        SchemaNode.Group storedEntries = group("key_value", REPEATED, string("key", REQUIRED), storedValue);
        ParquetSchema stored = schema(annotated("m", OPTIONAL, new LogicalType.MapType(), storedEntries));

        SchemaNode.Group value = group("value", OPTIONAL, standardList("codes", REQUIRED, int32("element", REQUIRED)));
        SchemaNode.Group entries = group("key_value", REPEATED, string("key", REQUIRED), value);
        assertThat(StandardLists.of(stored))
                .isEqualTo(schema(annotated("m", OPTIONAL, new LogicalType.MapType(), entries)));
    }

    @Test
    void variantGroupUnchanged() {
        SchemaNode.Group variant = annotated(
                "v", OPTIONAL, new LogicalType.Variant(), binary("metadata", REQUIRED), binary("value", REQUIRED));
        ParquetSchema stored = schema(variant);

        assertThat(StandardLists.of(stored)).isSameAs(stored);
    }

    @Test
    void repeatedFieldInAnElement() {
        SchemaNode.Group storedElement = group("element", OPTIONAL, int32("codes", REPEATED));
        ParquetSchema stored = schema(standardList("items", OPTIONAL, storedElement));

        SchemaNode.Group element =
                group("element", OPTIONAL, standardList("codes", REQUIRED, int32("element", REQUIRED)));
        assertThat(StandardLists.of(stored)).isEqualTo(schema(standardList("items", OPTIONAL, element)));
    }

    // The column chunks of the file stay bound to the leaves: the levels and the order of the leaves cannot change.
    @Test
    void leavesKeepTheirLevels() {
        ParquetSchema stored = schema(
                int32("Int32_list", REPEATED),
                group("phoneNumbers", OPTIONAL, group("phone", REPEATED, int64("number", REQUIRED))),
                list("a", REQUIRED, list("array", REPEATED, int32("array", REPEATED))),
                list("nums", OPTIONAL, int32("array", REPEATED)));

        ParquetSchema standard = StandardLists.of(stored);

        List<ColumnPath> storedLeaves = stored.leafColumns();
        List<ColumnPath> standardLeaves = standard.leafColumns();
        assertThat(standardLeaves).hasSameSizeAs(storedLeaves);
        for (int i = 0; i < storedLeaves.size(); i++) {
            assertThat(standard.maxLevels(standardLeaves.get(i)))
                    .as(standardLeaves.get(i).dot())
                    .isEqualTo(stored.maxLevels(storedLeaves.get(i)));
        }
    }

    // The list takes the field id of the repeated field; the element of a two-level list keeps its own.
    @Test
    void fieldIdsStayWithTheField() {
        SchemaNode.Primitive codes = withFieldId(int32("codes", REPEATED), 7);
        SchemaNode.Primitive storedElement = withFieldId(int32("array", REPEATED), 9);
        SchemaNode.Group nums = new SchemaNode.Group(
                "nums", OPTIONAL, List.of(storedElement), Optional.of(new LogicalType.ListType()), 8);

        ParquetSchema standard = StandardLists.of(schema(codes, nums));

        assertThat(standard.find(ColumnPath.of("codes")).orElseThrow().fieldId())
                .isEqualTo(7);
        assertThat(standard.find(ColumnPath.of("codes", "list", "element"))
                        .orElseThrow()
                        .fieldId())
                .isEqualTo(NO_FIELD_ID);
        assertThat(standard.find(ColumnPath.of("nums")).orElseThrow().fieldId()).isEqualTo(8);
        assertThat(standard.find(ColumnPath.of("nums", "list", "element"))
                        .orElseThrow()
                        .fieldId())
                .isEqualTo(9);
    }

    @Test
    void standardSchemaUnchanged() {
        ParquetSchema stored =
                schema(int32("id", REQUIRED), standardList("tags", OPTIONAL, string("element", OPTIONAL)));

        assertThat(StandardLists.of(stored)).isSameAs(stored);
    }

    private static ParquetSchema schema(SchemaNode... fields) {
        return new ParquetSchema(group("schema", REQUIRED, fields));
    }

    /** A list in the standard form: {@code <repetition> group name (LIST) { repeated group list { element } } }. */
    private static SchemaNode.Group standardList(String name, Repetition repetition, SchemaNode element) {
        return list(name, repetition, group("list", REPEATED, element));
    }

    private static SchemaNode.Group list(String name, Repetition repetition, SchemaNode repeatedChild) {
        return annotated(name, repetition, new LogicalType.ListType(), repeatedChild);
    }

    private static SchemaNode.Group annotated(
            String name, Repetition repetition, LogicalType logicalType, SchemaNode... children) {
        return new SchemaNode.Group(name, repetition, List.of(children), Optional.of(logicalType), NO_FIELD_ID);
    }

    private static SchemaNode.Group group(String name, Repetition repetition, SchemaNode... children) {
        return new SchemaNode.Group(name, repetition, List.of(children), Optional.empty(), NO_FIELD_ID);
    }

    private static SchemaNode.Primitive int32(String name, Repetition repetition) {
        return primitive(name, repetition, PrimitiveKind.INT32, Optional.empty());
    }

    private static SchemaNode.Primitive int64(String name, Repetition repetition) {
        return primitive(name, repetition, PrimitiveKind.INT64, Optional.empty());
    }

    private static SchemaNode.Primitive binary(String name, Repetition repetition) {
        return primitive(name, repetition, PrimitiveKind.BYTE_ARRAY, Optional.empty());
    }

    private static SchemaNode.Primitive string(String name, Repetition repetition) {
        return primitive(name, repetition, PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.StringType()));
    }

    private static SchemaNode.Primitive primitive(
            String name, Repetition repetition, PrimitiveKind kind, Optional<LogicalType> logicalType) {
        return new SchemaNode.Primitive(name, repetition, kind, OptionalInt.empty(), logicalType, NO_FIELD_ID);
    }

    private static SchemaNode.Primitive withFieldId(SchemaNode.Primitive primitive, int fieldId) {
        return new SchemaNode.Primitive(
                primitive.name(),
                primitive.repetition(),
                primitive.kind(),
                primitive.typeLength(),
                primitive.logicalType(),
                fieldId);
    }
}
