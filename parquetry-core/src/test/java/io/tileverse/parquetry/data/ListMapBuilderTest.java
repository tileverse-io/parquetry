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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.ListVector;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.MapVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.VectorArrays;

/**
 * Unit tests for LIST and MAP authoring in {@link ParquetRecordBatchBuilder}.
 *
 * <p>Each test authors a populated row, an empty container, and a null container, then asserts the frozen
 * {@link ListVector} / {@link MapVector} offsets, validity, and child vector shapes.
 */
class ListMapBuilderTest {

    /** {@code optional group nums (LIST) { repeated group list { optional int32 element; } }}. */
    @Test
    void listOfInt32PopulatedEmptyNull() {
        ParquetSchema schema = listOfInt32Schema();
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);

        // Row 0: [10, 20, 30].
        builder.beginList("nums").addInt(10).addInt(20).addInt(30).endList().endRow();
        // Row 1: empty list.
        builder.beginList("nums").endList().endRow();
        // Row 2: null list.
        builder.setNull(ColumnPath.of("nums")).endRow();

        ParquetRecordBatch batch = builder.build();
        assertThat(batch.rowCount()).isEqualTo(3);

        ListVector listVec = (ListVector) batch.columns().get(ColumnPath.of("nums"));
        assertThat(listVec.size()).isEqualTo(3);
        assertThat(listVec.offsets()).containsExactly(0, 3, 3, 3);
        assertThat(listVec.validity().isNull(0)).isFalse();
        assertThat(listVec.validity().isNull(1)).as("empty list is present").isFalse();
        assertThat(listVec.validity().isNull(2)).as("null list is absent").isTrue();

        IntVector elements = (IntVector) listVec.child();
        assertThat(elements.size()).isEqualTo(3);
        assertThat(VectorArrays.toArray(elements)).containsExactly(10, 20, 30);
    }

    /**
     * {@code optional group addresses (LIST) { repeated group list { optional group element { optional binary locality
     * (STRING); } } }}.
     */
    @Test
    void listOfStructPopulatedEmptyNull() {
        ParquetSchema schema = listOfStructSchema();
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);

        ColumnPath locality = ColumnPath.of("element", "locality");

        // Row 0: two struct elements, the second with a null locality.
        builder.beginList("addresses")
                .addElement()
                .setString(locality, "Rosario")
                .endElement()
                .addElement()
                .setNull(locality)
                .endElement()
                .endList()
                .endRow();
        // Row 1: empty list.
        builder.beginList("addresses").endList().endRow();
        // Row 2: null list.
        builder.setNull(ColumnPath.of("addresses")).endRow();

        ParquetRecordBatch batch = builder.build();
        assertThat(batch.rowCount()).isEqualTo(3);

        ListVector listVec = (ListVector) batch.columns().get(ColumnPath.of("addresses"));
        assertThat(listVec.offsets()).containsExactly(0, 2, 2, 2);
        assertThat(listVec.validity().isNull(1)).as("empty list present").isFalse();
        assertThat(listVec.validity().isNull(2)).as("null list absent").isTrue();

        StructVector elementVec = (StructVector) listVec.child();
        assertThat(elementVec.size()).isEqualTo(2);
        BinaryVector localityVec = (BinaryVector) elementVec.children().get(ColumnPath.of("locality"));
        assertThat(localityVec.isNull(0)).isFalse();
        assertThat(localityVec.isNull(1))
                .as("second element's locality is null")
                .isTrue();
    }

    /**
     * {@code optional group tags (MAP) { repeated group key_value { required binary key (STRING); optional binary value
     * (STRING); } }}.
     */
    @Test
    void mapStringStringPopulatedEmptyNull() {
        ParquetSchema schema = mapStringStringSchema();
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);

        ColumnPath keyPath = ColumnPath.of("key_value", "key");
        ColumnPath valuePath = ColumnPath.of("key_value", "value");

        // Row 0: {"a" -> "1", "b" -> null}.
        builder.beginMap("tags")
                .putEntry()
                .setString(keyPath, "a")
                .setString(valuePath, "1")
                .endEntry()
                .putEntry()
                .setString(keyPath, "b")
                .setNull(valuePath)
                .endEntry()
                .endMap()
                .endRow();
        // Row 1: empty map.
        builder.beginMap("tags").endMap().endRow();
        // Row 2: null map.
        builder.setNull(ColumnPath.of("tags")).endRow();

        ParquetRecordBatch batch = builder.build();
        assertThat(batch.rowCount()).isEqualTo(3);

        MapVector mapVec = (MapVector) batch.columns().get(ColumnPath.of("tags"));
        assertThat(mapVec.offsets()).containsExactly(0, 2, 2, 2);
        assertThat(mapVec.validity().isNull(1)).as("empty map present").isFalse();
        assertThat(mapVec.validity().isNull(2)).as("null map absent").isTrue();

        BinaryVector keys = (BinaryVector) mapVec.keys();
        BinaryVector values = (BinaryVector) mapVec.values();
        assertThat(keys.size()).isEqualTo(2);
        assertThat(values.isNull(0)).isFalse();
        assertThat(values.isNull(1)).as("second entry's value is null").isTrue();
    }

    /** A required list freezes as all-valid even for an empty container. */
    @Test
    void requiredListIsAllValid() {
        ParquetSchema schema = requiredListOfInt32Schema();
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);

        builder.beginList("nums").addInt(1).endList().endRow();
        builder.beginList("nums").endList().endRow();

        ParquetRecordBatch batch = builder.build();
        ListVector listVec = (ListVector) batch.columns().get(ColumnPath.of("nums"));
        assertThat(listVec.offsets()).containsExactly(0, 1, 1);
        assertThat(listVec.validity().isNull(0)).isFalse();
        assertThat(listVec.validity().isNull(1)).isFalse();
    }

    /** Each scalar list element adds its per-value byte estimate to the auto-flush threshold, like an index setter. */
    @Test
    void listElementsRaiseTheByteEstimate() {
        ParquetSchema schema = listOfInt32Schema();
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);

        builder.beginList("nums");
        int elementCount = 1000;
        for (int i = 0; i < elementCount; i++) {
            builder.addInt(i);
        }
        builder.endList().endRow();

        assertThat(builder.approxBatchBytes())
                .as("each int element adds Integer.BYTES to the running estimate")
                .isEqualTo((long) elementCount * Integer.BYTES);
    }

    /** Mixing a scalar add verb with an open explicit element scope is rejected before it double-commits. */
    @Test
    void scalarAddInsideOpenElementScopeIsRejected() {
        ParquetSchema schema = listOfInt32Schema();
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);

        builder.beginList("nums").addElement();

        assertThatThrownBy(() -> builder.addInt(5))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("addElement");
    }

    /**
     * A flat field after a struct-nested list, plus a top-level field after the struct, must keep their leaf indices
     * aligned with the schema's depth-first leaf order. A miscount of the nested list's leaves would route these
     * trailing scalar setters into the wrong accumulator.
     */
    @Test
    void trailingFlatFieldsAfterStructNestedListKeepLeafIndexAlignment() {
        ParquetSchema schema = structWithNestedListThenTrailingFlatsSchema();
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);

        ColumnPath phonesPath = ColumnPath.of("person", "phones");
        ColumnPath agePath = ColumnPath.of("person", "age");
        ColumnPath idPath = ColumnPath.of("id");

        builder.beginStruct("person");
        builder.beginList(phonesPath).addString("555-1234").endList();
        builder.setInt(agePath, 42);
        builder.endStruct();
        builder.setLong(idPath, 7L);
        builder.endRow();

        ParquetRecordBatch batch = builder.build();

        StructVector person = (StructVector) batch.columns().get(ColumnPath.of("person"));
        IntVector age = (IntVector) person.children().get(ColumnPath.of("age"));
        assertThat(age.getInt(0))
                .as("age lands in its own slot, not the list's")
                .isEqualTo(42);

        ListVector phones = (ListVector) person.children().get(ColumnPath.of("phones"));
        assertThat(phones.offsets()).containsExactly(0, 1);

        LongVector id = (LongVector) batch.columns().get(ColumnPath.of("id"));
        assertThat(id.getLong(0))
                .as("the top-level field after the struct stays aligned")
                .isEqualTo(7L);
    }

    /** A REQUIRED list left unset for a row is a programming error, like an unset required flat column. */
    @Test
    void unsetRequiredListThrows() {
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(requiredListOfInt32Schema());
        assertThatThrownBy(builder::endRow)
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("Required column 'nums' was not set");
    }

    /** A REQUIRED map left unset for a row is rejected. */
    @Test
    void unsetRequiredMapThrows() {
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(requiredMapStringStringSchema());
        assertThatThrownBy(builder::endRow)
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("Required column 'tags' was not set");
    }

    /** A REQUIRED Variant column left unset for a row is rejected. */
    @Test
    void unsetRequiredVariantThrows() {
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(requiredVariantSchema());
        assertThatThrownBy(builder::endRow)
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("Required column 'v' was not set");
    }

    /** A REQUIRED list nested inside a PRESENT struct, left unset, is rejected like a top-level required container. */
    @Test
    void unsetRequiredListNestedInPresentStructThrows() {
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(structWithRequiredNestedListSchema());
        builder.beginStruct("s").setString(ColumnPath.of("s", "name"), "x").endStruct();
        assertThatThrownBy(builder::endRow)
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("nums");
    }

    /** A null struct skips the nested required-container check: its children are nulled by the parent. */
    @Test
    void nullStructSkipsNestedRequiredContainerCheck() {
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(structWithRequiredNestedListSchema());
        builder.setNull(ColumnPath.of("s")).endRow();
        ParquetRecordBatch batch = builder.build();
        StructVector person = (StructVector) batch.columns().get(ColumnPath.of("s"));
        assertThat(person.validity().isNull(0)).as("the absent struct is null").isTrue();
    }

    /** A present-empty required list nested in a struct is valid: it was authored with beginList/endList. */
    @Test
    void presentEmptyRequiredListNestedInStructIsValid() {
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(structWithRequiredNestedListSchema());
        builder.beginStruct("s")
                .setString(ColumnPath.of("s", "name"), "x")
                .beginList(ColumnPath.of("s", "nums"))
                .endList()
                .endStruct()
                .endRow();
        ParquetRecordBatch batch = builder.build();
        StructVector person = (StructVector) batch.columns().get(ColumnPath.of("s"));
        ListVector nums = (ListVector) person.children().get(ColumnPath.of("nums"));
        assertThat(nums.validity().isNull(0))
                .as("the present empty list is valid")
                .isFalse();
    }

    /** An OPTIONAL container left unset freezes as null, unchanged by the required-container check. */
    @Test
    void unsetOptionalListIsNull() {
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(listOfInt32Schema());
        builder.endRow();
        ParquetRecordBatch batch = builder.build();
        ListVector listVec = (ListVector) batch.columns().get(ColumnPath.of("nums"));
        assertThat(listVec.validity().isNull(0)).isTrue();
    }

    // --- schema helpers ---

    private static ParquetSchema listOfInt32Schema() {
        SchemaNode.Primitive element = new SchemaNode.Primitive(
                "element", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group listGroup = new SchemaNode.Group(
                "nums", Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
        return rootOf(listGroup);
    }

    private static ParquetSchema requiredListOfInt32Schema() {
        SchemaNode.Primitive element = new SchemaNode.Primitive(
                "element", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group listGroup = new SchemaNode.Group(
                "nums", Repetition.REQUIRED, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
        return rootOf(listGroup);
    }

    private static ParquetSchema listOfStructSchema() {
        SchemaNode.Primitive locality = new SchemaNode.Primitive(
                "locality",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
        SchemaNode.Group element =
                new SchemaNode.Group("element", Repetition.OPTIONAL, List.of(locality), Optional.empty(), -1);
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group listGroup = new SchemaNode.Group(
                "addresses", Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
        return rootOf(listGroup);
    }

    private static ParquetSchema requiredMapStringStringSchema() {
        SchemaNode.Primitive key = new SchemaNode.Primitive(
                "key",
                Repetition.REQUIRED,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
        SchemaNode.Primitive value = new SchemaNode.Primitive(
                "value",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
        SchemaNode.Group keyValue =
                new SchemaNode.Group("key_value", Repetition.REPEATED, List.of(key, value), Optional.empty(), -1);
        SchemaNode.Group mapGroup = new SchemaNode.Group(
                "tags", Repetition.REQUIRED, List.of(keyValue), Optional.of(new LogicalType.MapType()), -1);
        return rootOf(mapGroup);
    }

    private static ParquetSchema requiredVariantSchema() {
        SchemaNode.Primitive metadata = new SchemaNode.Primitive(
                "metadata", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive value = new SchemaNode.Primitive(
                "value", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group variant = new SchemaNode.Group(
                "v", Repetition.REQUIRED, List.of(metadata, value), Optional.of(new LogicalType.Variant()), -1);
        return rootOf(variant);
    }

    private static ParquetSchema mapStringStringSchema() {
        SchemaNode.Primitive key = new SchemaNode.Primitive(
                "key",
                Repetition.REQUIRED,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
        SchemaNode.Primitive value = new SchemaNode.Primitive(
                "value",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
        SchemaNode.Group keyValue =
                new SchemaNode.Group("key_value", Repetition.REPEATED, List.of(key, value), Optional.empty(), -1);
        SchemaNode.Group mapGroup = new SchemaNode.Group(
                "tags", Repetition.OPTIONAL, List.of(keyValue), Optional.of(new LogicalType.MapType()), -1);
        return rootOf(mapGroup);
    }

    private static ParquetSchema structWithNestedListThenTrailingFlatsSchema() {
        SchemaNode.Primitive element = new SchemaNode.Primitive(
                "element",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group phones = new SchemaNode.Group(
                "phones", Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
        SchemaNode.Primitive age = new SchemaNode.Primitive(
                "age", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group person =
                new SchemaNode.Group("person", Repetition.OPTIONAL, List.of(phones, age), Optional.empty(), -1);
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(person, id), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static ParquetSchema structWithRequiredNestedListSchema() {
        SchemaNode.Primitive element = new SchemaNode.Primitive(
                "element", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group nums = new SchemaNode.Group(
                "nums", Repetition.REQUIRED, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
        SchemaNode.Primitive name = new SchemaNode.Primitive(
                "name",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
        SchemaNode.Group struct =
                new SchemaNode.Group("s", Repetition.OPTIONAL, List.of(nums, name), Optional.empty(), -1);
        return rootOf(struct);
    }

    private static ParquetSchema rootOf(SchemaNode topLevel) {
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(topLevel), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
