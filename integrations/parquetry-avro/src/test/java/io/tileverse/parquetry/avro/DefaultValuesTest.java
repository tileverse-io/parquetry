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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DefaultValuesTest {

    static Stream<Arguments> scalarDefaults() {
        return Stream.of(
                Arguments.of(AvroSchema.INT, 7L, 7),
                Arguments.of(AvroSchema.LONG, 7L, 7L),
                Arguments.of(AvroSchema.FLOAT, 7L, 7.0f),
                Arguments.of(AvroSchema.FLOAT, 1.5d, 1.5f),
                Arguments.of(AvroSchema.DOUBLE, 7L, 7.0d),
                Arguments.of(AvroSchema.DOUBLE, 1.5d, 1.5d),
                Arguments.of(AvroSchema.BOOLEAN, true, true),
                Arguments.of(AvroSchema.STRING, "hi", "hi"));
    }

    @ParameterizedTest
    @MethodSource("scalarDefaults")
    void materializesScalarDefault(AvroSchema schema, Object jsonDefault, Object expected) {
        Object value = DefaultValues.materialize(schema, jsonDefault);
        assertThat(value).isEqualTo(expected).hasSameClassAs(expected);
    }

    @Test
    void materializesNullTypeDefault() {
        assertThat(DefaultValues.materialize(AvroSchema.NULL, null)).isNull();
    }

    @Test
    void rejectsIntDefaultOutOfRange() {
        assertThatThrownBy(() -> DefaultValues.materialize(AvroSchema.INT, 5_000_000_000L))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("5000000000");
    }

    @Test
    void materializesBytesFromCharsAsBytesString() {
        Object value = DefaultValues.materialize(AvroSchema.BYTES, "\u00FF\u0000");
        assertThat(value).isInstanceOf(MemorySegment.class);
        assertThat(((MemorySegment) value).isReadOnly()).isTrue();
        assertThat(bytesOf(value)).containsExactly(-1, 0);
    }

    @Test
    void materializesFixedFromCharsAsBytesString() {
        Object value = DefaultValues.materialize(fixed(2), "\u00FF\u0000");
        assertThat(bytesOf(value)).containsExactly(-1, 0);
    }

    @Test
    void rejectsFixedDefaultWithWrongLength() {
        AvroSchema.Fixed schema = fixed(2);
        assertThatThrownBy(() -> DefaultValues.materialize(schema, "\u00FF"))
                .isInstanceOf(AvroFormatException.class)
                .hasMessage("Fixed ns.F default has length 1, expected 2");
    }

    @Test
    void rejectsBinaryDefaultWithCodePointAbove255() {
        assertThatThrownBy(() -> DefaultValues.materialize(AvroSchema.BYTES, "\u0100"))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("255");
    }

    @Test
    void materializesEnumSymbol() {
        assertThat(DefaultValues.materialize(suit(), "SPADES")).isEqualTo("SPADES");
    }

    @Test
    void rejectsSymbolMissingFromEnum() {
        AvroSchema.Enum schema = suit();
        assertThatThrownBy(() -> DefaultValues.materialize(schema, "CLUBS"))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("ns.Suit")
                .hasMessageContaining("CLUBS");
    }

    @Test
    void materializesIntArrayDefault() {
        AvroSchema.Array schema = new AvroSchema.Array(AvroSchema.INT);
        Object value = DefaultValues.materialize(schema, List.of(1L, 2L, 3L));
        assertThat(value).isEqualTo(List.of(1, 2, 3));
    }

    @Test
    void materializedArrayDefaultIsUnmodifiable() {
        AvroSchema.Array schema = new AvroSchema.Array(AvroSchema.INT);
        Object value = DefaultValues.materialize(schema, List.of(1L));
        @SuppressWarnings("unchecked")
        List<Object> list = (List<Object>) value;
        assertThatThrownBy(() -> list.add(4)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void materializedArrayDefaultAllowsNullElements() {
        AvroSchema.Union nullableInt = new AvroSchema.Union(List.of(AvroSchema.NULL, AvroSchema.INT));
        AvroSchema.Array schema = new AvroSchema.Array(nullableInt);
        Object value = DefaultValues.materialize(schema, Arrays.asList(null, null));
        assertThat(value).isEqualTo(Arrays.asList(null, null));
    }

    @Test
    void rejectsNonNullDefaultForNullFirstUnion() {
        AvroSchema.Union nullableInt = new AvroSchema.Union(List.of(AvroSchema.NULL, AvroSchema.INT));
        assertThatThrownBy(() -> DefaultValues.materialize(nullableInt, 5L))
                .isInstanceOf(AvroFormatException.class)
                .hasMessage("Default value 5 is not a null");
    }

    @Test
    void materializesMapDefault() {
        AvroSchema.Map schema = new AvroSchema.Map(AvroSchema.LONG);
        Object value = DefaultValues.materialize(schema, jsonObject("a", 1L));
        assertThat(value).isEqualTo(Map.of("a", 1L));
    }

    @Test
    void absentNestedFieldFallsBackToItsOwnDefault() {
        AvroRecord materialized = (AvroRecord) DefaultValues.materialize(recordXY(), jsonObject("x", 1L));
        assertThat(materialized.get("x")).isEqualTo(1);
        assertThat(materialized.get("y")).isEqualTo("d");
    }

    @Test
    void ignoresUnknownKeysInRecordDefault() {
        LinkedHashMap<String, Object> object = jsonObject("x", 1L);
        object.put("unknown", true);
        AvroRecord materialized = (AvroRecord) DefaultValues.materialize(recordXY(), object);
        assertThat(materialized.get("x")).isEqualTo(1);
        assertThat(materialized.get("y")).isEqualTo("d");
    }

    @Test
    void rejectsRecordDefaultMissingFieldWithoutDefault() {
        AvroSchema.Record schema = recordXY();
        LinkedHashMap<String, Object> missingX = new LinkedHashMap<>();
        assertThatThrownBy(() -> DefaultValues.materialize(schema, missingX))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("ns.Point")
                .hasMessageContaining("x");
    }

    @Test
    void materializesNullDefaultForNullFirstUnion() {
        AvroSchema.Union union = new AvroSchema.Union(List.of(AvroSchema.NULL, AvroSchema.STRING));
        assertThat(DefaultValues.materialize(union, null)).isNull();
    }

    @Test
    void typesUnionDefaultByFirstBranch() {
        AvroSchema.Union union = new AvroSchema.Union(List.of(AvroSchema.INT, AvroSchema.STRING));
        Object value = DefaultValues.materialize(union, 7L);
        assertThat(value).isEqualTo(7).isInstanceOf(Integer.class);
    }

    @Test
    void convertsDecimalBytesDefault() {
        AvroSchema schema = new AvroSchema.Primitive(AvroSchema.Type.BYTES, new LogicalType.Decimal(4, 2));
        Object value = DefaultValues.materialize(schema, "09");
        assertThat(value).isEqualTo(new BigDecimal("123.45"));
    }

    @Test
    void convertsDateIntDefault() {
        AvroSchema schema = new AvroSchema.Primitive(AvroSchema.Type.INT, LogicalType.Date.INSTANCE);
        Object value = DefaultValues.materialize(schema, 19000L);
        assertThat(value).isEqualTo(LocalDate.of(2022, 1, 8));
    }

    @Test
    void convertsFirstBranchLogicalType() {
        AvroSchema date = new AvroSchema.Primitive(AvroSchema.Type.INT, LogicalType.Date.INSTANCE);
        AvroSchema.Union union = new AvroSchema.Union(List.of(date, AvroSchema.NULL));
        Object value = DefaultValues.materialize(union, 19000L);
        assertThat(value).isEqualTo(LocalDate.of(2022, 1, 8));
    }

    @Test
    void convertsLogicalTypeExactlyOnceThroughRef() {
        AvroSchema.Fixed decimalFixed =
                new AvroSchema.Fixed("Money", "ns", List.of(), 2, new LogicalType.Decimal(4, 2));
        AvroSchema.Ref ref = new AvroSchema.Ref(decimalFixed.fullName());
        ref.bind(decimalFixed);
        // Ref.logicalType() delegates to its target; a second conversion on the produced
        // BigDecimal would throw ClassCastException
        Object value = DefaultValues.materialize(ref, "09");
        assertThat(value).isEqualTo(new BigDecimal("123.45"));
    }

    @Test
    void materializesThroughBoundRef() {
        AvroSchema.Record target = recordXY();
        AvroSchema.Ref ref = new AvroSchema.Ref(target.fullName());
        ref.bind(target);
        AvroRecord materialized = (AvroRecord) DefaultValues.materialize(ref, jsonObject("x", 1L));
        assertThat(materialized.get("x")).isEqualTo(1);
        assertThat(materialized.get("y")).isEqualTo("d");
    }

    static Stream<Arguments> mismatchedDefaults() {
        return Stream.of(
                Arguments.of(AvroSchema.INT, "seven"),
                Arguments.of(AvroSchema.LONG, true),
                Arguments.of(AvroSchema.FLOAT, "1.5"),
                Arguments.of(AvroSchema.DOUBLE, false),
                Arguments.of(AvroSchema.BOOLEAN, 1L),
                Arguments.of(AvroSchema.STRING, 7L),
                Arguments.of(AvroSchema.NULL, 0L),
                Arguments.of(AvroSchema.BYTES, 7L),
                Arguments.of(fixed(2), 7L),
                Arguments.of(suit(), 7L),
                Arguments.of(new AvroSchema.Array(AvroSchema.INT), 7L),
                Arguments.of(new AvroSchema.Map(AvroSchema.LONG), List.of()),
                Arguments.of(recordXY(), 7L));
    }

    @ParameterizedTest
    @MethodSource("mismatchedDefaults")
    void rejectsMismatchedDefault(AvroSchema schema, Object jsonDefault) {
        assertThatThrownBy(() -> DefaultValues.materialize(schema, jsonDefault))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("is not a");
    }

    private static byte[] bytesOf(Object value) {
        return ((MemorySegment) value).toArray(ValueLayout.JAVA_BYTE);
    }

    private static AvroSchema.Fixed fixed(int size) {
        return new AvroSchema.Fixed("F", "ns", List.of(), size, null);
    }

    private static AvroSchema.Enum suit() {
        return new AvroSchema.Enum("Suit", "ns", List.of(), List.of("HEARTS", "SPADES"), null);
    }

    private static AvroSchema.Record recordXY() {
        AvroSchema.Field x = AvroSchema.Field.of("x", 0, AvroSchema.INT);
        AvroSchema.Field y = new AvroSchema.Field("y", 1, AvroSchema.STRING, "d", true, List.of(), Map.of());
        return new AvroSchema.Record("Point", "ns", List.of(), List.of(x, y));
    }

    private static LinkedHashMap<String, Object> jsonObject(String key, Object value) {
        LinkedHashMap<String, Object> object = new LinkedHashMap<>();
        object.put(key, value);
        return object;
    }
}
