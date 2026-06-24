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
package io.tileverse.parquetry.internal.write;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.BooleanVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DoubleVector;
import io.tileverse.parquetry.batch.FixedLenBinaryVector;
import io.tileverse.parquetry.batch.FloatVector;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.LongVector;
import io.tileverse.parquetry.batch.VariantVector;
import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class ColumnAccumulatorTest {

    @ParameterizedTest
    @MethodSource("primitiveKinds")
    void freezesOneValueAndOneNull(PrimitiveKind kind) {
        ColumnAccumulator acc = ColumnAccumulator.forKind(kind, 0);
        setTypedValue(acc, kind);
        acc.endRow();
        acc.setNull();
        acc.endRow();

        ColumnVector vector = acc.freeze();

        assertThat(vector.size()).isEqualTo(2);
        assertThat(vector.isValid(0)).isTrue();
        assertThat(vector.isNull(1)).isTrue();
        assertTypedValue(vector, kind);
    }

    @Test
    void growsBufferBeyondInitialCapacityAndTrimsOnFreeze() {
        int rowCount = 40;
        int nullRow = 20;
        ColumnAccumulator acc = ColumnAccumulator.forKind(PrimitiveKind.INT32, 0);
        for (int row = 0; row < rowCount; row++) {
            if (row == nullRow) {
                acc.setNull();
            } else {
                acc.setInt(row * 10);
            }
            acc.endRow();
        }

        IntVector vector = (IntVector) acc.freeze();

        assertThat(vector.size()).isEqualTo(rowCount);
        assertThat(vector.getInt(0)).isZero();
        assertThat(vector.getInt(17)).isEqualTo(170);
        assertThat(vector.getInt(39)).isEqualTo(390);
        assertThat(vector.isNull(nullRow)).isTrue();
    }

    @Test
    void binaryAccumulatorFreezesSegmentsAndNulls() {
        ColumnAccumulator acc = ColumnAccumulator.forKind(PrimitiveKind.BYTE_ARRAY, 0);
        acc.setBinary(MemorySegment.ofArray("ab".getBytes(StandardCharsets.UTF_8)));
        acc.endRow();
        acc.setBinary(MemorySegment.ofArray("cde".getBytes(StandardCharsets.UTF_8)));
        acc.endRow();
        acc.setNull();
        acc.endRow();

        BinaryVector vector = (BinaryVector) acc.freeze();

        assertThat(vector.size()).isEqualTo(3);
        assertThat(vector.get(0).toArray(ValueLayout.JAVA_BYTE)).isEqualTo("ab".getBytes(StandardCharsets.UTF_8));
        assertThat(vector.get(1).toArray(ValueLayout.JAVA_BYTE)).isEqualTo("cde".getBytes(StandardCharsets.UTF_8));
        assertThat(vector.isNull(2)).isTrue();
    }

    @Test
    void fixedLenAccumulatorRejectsWrongWidth() {
        ColumnAccumulator acc = ColumnAccumulator.forKind(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, 4);
        MemorySegment wrongWidth = MemorySegment.ofArray(new byte[3]);
        assertThatThrownBy(() -> acc.setBinary(wrongWidth))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("width");
    }

    @Test
    void fixedLenAccumulatorFreezesFullSlots() {
        ColumnAccumulator acc = ColumnAccumulator.forKind(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, 4);
        acc.setBinary(MemorySegment.ofArray(new byte[] {1, 2, 3, 4}));
        acc.endRow();
        acc.setNull();
        acc.endRow();

        FixedLenBinaryVector vector = (FixedLenBinaryVector) acc.freeze();

        assertThat(vector.size()).isEqualTo(2);
        assertThat(vector.byteWidth()).isEqualTo(4);
        assertThat(vector.isNull(1)).isTrue();
        assertThat(vector.get(0).toArray(ValueLayout.JAVA_BYTE)).isEqualTo(new byte[] {1, 2, 3, 4});
    }

    @Test
    void binaryGrowableBackingMixesPresentAndNull() {
        ColumnAccumulator acc = ColumnAccumulator.forKind(PrimitiveKind.BYTE_ARRAY, 0);
        acc.setBinary(MemorySegment.ofArray("first".getBytes(StandardCharsets.UTF_8)));
        acc.endRow();
        acc.setNull();
        acc.endRow();
        acc.setBinary(MemorySegment.ofArray("third".getBytes(StandardCharsets.UTF_8)));
        acc.endRow();

        BinaryVector vector = (BinaryVector) acc.freeze();

        assertThat(vector.size()).isEqualTo(3);
        assertThat(vector.get(0).toArray(ValueLayout.JAVA_BYTE)).isEqualTo("first".getBytes(StandardCharsets.UTF_8));
        assertThat(vector.isNull(1)).isTrue();
        assertThat(vector.get(2).toArray(ValueLayout.JAVA_BYTE)).isEqualTo("third".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void fixedLenNullSlotIsZeroed() {
        ColumnAccumulator acc = ColumnAccumulator.forKind(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, 4);
        acc.setBinary(MemorySegment.ofArray(new byte[] {9, 9, 9, 9}));
        acc.endRow();
        acc.setNull();
        acc.endRow();
        acc.setBinary(MemorySegment.ofArray(new byte[] {1, 2, 3, 4}));
        acc.endRow();

        FixedLenBinaryVector vector = (FixedLenBinaryVector) acc.freeze();

        assertThat(vector.size()).isEqualTo(3);
        assertThat(vector.isNull(1)).isTrue();
        assertThat(vector.consolidatedBacking().asSlice(4, 4).toArray(ValueLayout.JAVA_BYTE))
                .isEqualTo(new byte[] {0, 0, 0, 0});
        assertThat(vector.get(2).toArray(ValueLayout.JAVA_BYTE)).isEqualTo(new byte[] {1, 2, 3, 4});
    }

    @Test
    void clearThenReuseDoesNotCorruptEarlierBinaryVector() {
        ColumnAccumulator acc = ColumnAccumulator.forKind(PrimitiveKind.BYTE_ARRAY, 0);
        acc.setBinary(MemorySegment.ofArray("ab".getBytes(StandardCharsets.UTF_8)));
        acc.endRow();
        acc.setNull();
        acc.endRow();
        BinaryVector vectorA = (BinaryVector) acc.freeze();

        acc.clear();
        acc.setBinary(MemorySegment.ofArray("xyzw".getBytes(StandardCharsets.UTF_8)));
        acc.endRow();
        acc.setBinary(MemorySegment.ofArray("q".getBytes(StandardCharsets.UTF_8)));
        acc.endRow();
        BinaryVector vectorB = (BinaryVector) acc.freeze();

        assertThat(vectorA.size()).isEqualTo(2);
        assertThat(vectorA.get(0).toArray(ValueLayout.JAVA_BYTE)).isEqualTo("ab".getBytes(StandardCharsets.UTF_8));
        assertThat(vectorA.isNull(1)).isTrue();

        assertThat(vectorB.size()).isEqualTo(2);
        assertThat(vectorB.get(0).toArray(ValueLayout.JAVA_BYTE)).isEqualTo("xyzw".getBytes(StandardCharsets.UTF_8));
        assertThat(vectorB.get(1).toArray(ValueLayout.JAVA_BYTE)).isEqualTo("q".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void clearThenReuseDoesNotCorruptEarlierPrimitiveVector() {
        ColumnAccumulator acc = ColumnAccumulator.forKind(PrimitiveKind.INT32, 0);
        acc.setInt(11);
        acc.endRow();
        acc.setNull();
        acc.endRow();
        IntVector vectorA = (IntVector) acc.freeze();

        acc.clear();
        acc.setInt(22);
        acc.endRow();
        acc.setInt(33);
        acc.endRow();
        IntVector vectorB = (IntVector) acc.freeze();

        assertThat(vectorA.size()).isEqualTo(2);
        assertThat(vectorA.getInt(0)).isEqualTo(11);
        assertThat(vectorA.isNull(1)).isTrue();

        assertThat(vectorB.size()).isEqualTo(2);
        assertThat(vectorB.getInt(0)).isEqualTo(22);
        assertThat(vectorB.getInt(1)).isEqualTo(33);
        assertThat(vectorB.isValid(1)).isTrue();
    }

    @Test
    void clearThenReuseZeroesFixedLenNullSlot() {
        ColumnAccumulator acc = ColumnAccumulator.forKind(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, 4);
        acc.setBinary(MemorySegment.ofArray(new byte[] {9, 9, 9, 9}));
        acc.endRow();
        FixedLenBinaryVector vectorA = (FixedLenBinaryVector) acc.freeze();

        acc.clear();
        acc.setNull();
        acc.endRow();
        FixedLenBinaryVector vectorB = (FixedLenBinaryVector) acc.freeze();

        assertThat(vectorB.size()).isEqualTo(1);
        assertThat(vectorB.isNull(0)).isTrue();
        assertThat(vectorB.consolidatedBacking().asSlice(0, 4).toArray(ValueLayout.JAVA_BYTE))
                .isEqualTo(new byte[] {0, 0, 0, 0});
        assertThat(vectorA.get(0).toArray(ValueLayout.JAVA_BYTE)).isEqualTo(new byte[] {9, 9, 9, 9});
    }

    @Test
    void clearThenReuseWithFewerShorterBinaryCellsLeavesNoStaleTail() {
        ColumnAccumulator acc = ColumnAccumulator.forKind(PrimitiveKind.BYTE_ARRAY, 0);
        acc.setBinary(MemorySegment.ofArray("aaaaaaaa".getBytes(StandardCharsets.UTF_8)));
        acc.endRow();
        acc.setBinary(MemorySegment.ofArray("bbbbbbbb".getBytes(StandardCharsets.UTF_8)));
        acc.endRow();
        acc.setBinary(MemorySegment.ofArray("cccccccc".getBytes(StandardCharsets.UTF_8)));
        acc.endRow();
        BinaryVector vectorA = (BinaryVector) acc.freeze();

        acc.clear();
        acc.setBinary(MemorySegment.ofArray("z".getBytes(StandardCharsets.UTF_8)));
        acc.endRow();
        BinaryVector vectorB = (BinaryVector) acc.freeze();

        assertThat(vectorB.size()).isEqualTo(1);
        assertThat(vectorB.get(0).toArray(ValueLayout.JAVA_BYTE)).isEqualTo("z".getBytes(StandardCharsets.UTF_8));

        assertThat(vectorA.size()).isEqualTo(3);
        assertThat(vectorA.get(0).toArray(ValueLayout.JAVA_BYTE))
                .isEqualTo("aaaaaaaa".getBytes(StandardCharsets.UTF_8));
        assertThat(vectorA.get(1).toArray(ValueLayout.JAVA_BYTE))
                .isEqualTo("bbbbbbbb".getBytes(StandardCharsets.UTF_8));
        assertThat(vectorA.get(2).toArray(ValueLayout.JAVA_BYTE))
                .isEqualTo("cccccccc".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void variantAccumulatorFreezesMetadataValueAndValidity() {
        ColumnAccumulator.VariantAccumulator acc =
                (ColumnAccumulator.VariantAccumulator) ColumnAccumulator.forNode(optionalVariantNode());

        // Row 0: present Variant.
        acc.setVariant(utf8("meta0"), utf8("val0"));
        acc.endRow();
        // Row 1: null Variant.
        acc.setNull();
        acc.endRow();

        VariantVector vector = (VariantVector) acc.freeze();
        assertThat(vector.size()).isEqualTo(2);
        assertThat(vector.validity().isValid(0)).isTrue();
        assertThat(vector.validity().isNull(1)).isTrue();
        assertThat(asString(vector.metadataColumn().get(0))).isEqualTo("meta0");
        assertThat(asString(vector.valueColumn().get(0))).isEqualTo("val0");
    }

    @Test
    void requiredVariantAccumulatorIsAllValid() {
        ColumnAccumulator.VariantAccumulator acc =
                (ColumnAccumulator.VariantAccumulator) ColumnAccumulator.forNode(requiredVariantNode());

        acc.setVariant(utf8("m"), utf8("v"));
        acc.endRow();

        VariantVector vector = (VariantVector) acc.freeze();
        assertThat(vector.size()).isEqualTo(1);
        assertThat(vector.validity().isValid(0)).isTrue();
    }

    private static SchemaNode.Group optionalVariantNode() {
        return variantNode(Repetition.OPTIONAL);
    }

    private static SchemaNode.Group requiredVariantNode() {
        return variantNode(Repetition.REQUIRED);
    }

    private static SchemaNode.Group variantNode(Repetition repetition) {
        SchemaNode.Primitive metadata = new SchemaNode.Primitive(
                "metadata", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive value = new SchemaNode.Primitive(
                "value", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        return new SchemaNode.Group(
                "v", repetition, List.of(metadata, value), Optional.of(new LogicalType.Variant()), -1);
    }

    private static MemorySegment utf8(String text) {
        return MemorySegment.ofArray(text.getBytes(StandardCharsets.UTF_8)).asReadOnly();
    }

    private static String asString(MemorySegment segment) {
        return new String(segment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    @Test
    void wrongTypedSetterThrows() {
        ColumnAccumulator acc = ColumnAccumulator.forKind(PrimitiveKind.INT32, 0);
        assertThatThrownBy(() -> acc.setLong(1L)).isInstanceOf(ParquetWriteException.class);
    }

    private static Stream<Arguments> primitiveKinds() {
        return Stream.of(
                Arguments.of(PrimitiveKind.BOOLEAN),
                Arguments.of(PrimitiveKind.INT32),
                Arguments.of(PrimitiveKind.INT64),
                Arguments.of(PrimitiveKind.FLOAT),
                Arguments.of(PrimitiveKind.DOUBLE));
    }

    private static void setTypedValue(ColumnAccumulator acc, PrimitiveKind kind) {
        switch (kind) {
            case BOOLEAN -> acc.setBoolean(true);
            case INT32 -> acc.setInt(7);
            case INT64 -> acc.setLong(7L);
            case FLOAT -> acc.setFloat(7.0f);
            case DOUBLE -> acc.setDouble(7.0d);
            default -> throw new IllegalArgumentException("unsupported kind " + kind);
        }
    }

    private static void assertTypedValue(ColumnVector vector, PrimitiveKind kind) {
        switch (kind) {
            case BOOLEAN -> assertThat(((BooleanVector) vector).getBoolean(0)).isTrue();
            case INT32 -> assertThat(((IntVector) vector).getInt(0)).isEqualTo(7);
            case INT64 -> assertThat(((LongVector) vector).getLong(0)).isEqualTo(7L);
            case FLOAT -> assertThat(((FloatVector) vector).getFloat(0)).isEqualTo(7.0f);
            case DOUBLE -> assertThat(((DoubleVector) vector).getDouble(0)).isEqualTo(7.0d);
            default -> throw new IllegalArgumentException("unsupported kind " + kind);
        }
    }
}
