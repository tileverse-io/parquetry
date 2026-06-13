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
package io.tileverse.parquetry.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.ConvertedType;
import io.tileverse.parquetry.format.FieldRepetitionType;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.SchemaElement;

/**
 * Backward compatibility: a leaf annotated only with the deprecated {@link ConvertedType} (no modern
 * {@link LogicalType}) must still gain the equivalent logical type. Files written by duckdb, older parquet-mr, and
 * other legacy writers carry only the converted type; without this backfill a {@code UTF8} string column reads as raw
 * binary, and a {@code LIST} / {@code MAP} group is misclassified as a plain struct.
 */
class SchemaBuilderConvertedTypeTest {

    @ParameterizedTest(name = "{0} backfills to {2}")
    @MethodSource("convertedTypeBackfills")
    void backfillsLogicalTypeFromConvertedType(
            ConvertedType convertedType, PhysicalType physical, LogicalType expected) {
        Optional<LogicalType> logicalType = leafLogicalType(physical, Optional.of(convertedType), Optional.empty());

        assertThat(logicalType).contains(expected);
    }

    static Stream<Arguments> convertedTypeBackfills() {
        return Stream.of(
                Arguments.of(ConvertedType.UTF8, PhysicalType.BYTE_ARRAY, new LogicalType.StringType()),
                Arguments.of(ConvertedType.ENUM, PhysicalType.BYTE_ARRAY, new LogicalType.EnumType()),
                Arguments.of(ConvertedType.JSON, PhysicalType.BYTE_ARRAY, new LogicalType.JsonType()),
                Arguments.of(ConvertedType.BSON, PhysicalType.BYTE_ARRAY, new LogicalType.BsonType()),
                Arguments.of(ConvertedType.DATE, PhysicalType.INT32, new LogicalType.DateType()),
                Arguments.of(
                        ConvertedType.TIME_MILLIS,
                        PhysicalType.INT32,
                        new LogicalType.Time(true, LogicalType.TimeUnit.MILLIS)),
                Arguments.of(
                        ConvertedType.TIME_MICROS,
                        PhysicalType.INT64,
                        new LogicalType.Time(true, LogicalType.TimeUnit.MICROS)),
                Arguments.of(
                        ConvertedType.TIMESTAMP_MILLIS,
                        PhysicalType.INT64,
                        new LogicalType.Timestamp(true, LogicalType.TimeUnit.MILLIS)),
                Arguments.of(
                        ConvertedType.TIMESTAMP_MICROS,
                        PhysicalType.INT64,
                        new LogicalType.Timestamp(true, LogicalType.TimeUnit.MICROS)),
                Arguments.of(ConvertedType.INT_8, PhysicalType.INT32, new LogicalType.IntType((byte) 8, true)),
                Arguments.of(ConvertedType.INT_16, PhysicalType.INT32, new LogicalType.IntType((byte) 16, true)),
                Arguments.of(ConvertedType.INT_32, PhysicalType.INT32, new LogicalType.IntType((byte) 32, true)),
                Arguments.of(ConvertedType.INT_64, PhysicalType.INT64, new LogicalType.IntType((byte) 64, true)),
                Arguments.of(ConvertedType.UINT_8, PhysicalType.INT32, new LogicalType.IntType((byte) 8, false)),
                Arguments.of(ConvertedType.UINT_16, PhysicalType.INT32, new LogicalType.IntType((byte) 16, false)),
                Arguments.of(ConvertedType.UINT_32, PhysicalType.INT32, new LogicalType.IntType((byte) 32, false)),
                Arguments.of(ConvertedType.UINT_64, PhysicalType.INT64, new LogicalType.IntType((byte) 64, false)));
    }

    @Test
    void backfillsDecimalWithScaleAndPrecision() {
        SchemaElement leaf = leaf(
                PhysicalType.FIXED_LEN_BYTE_ARRAY,
                Optional.of(ConvertedType.DECIMAL),
                OptionalInt.of(2),
                OptionalInt.of(9));

        Optional<LogicalType> logicalType = build(leaf);

        assertThat(logicalType).contains(new LogicalType.Decimal(2, 9));
    }

    @Test
    void modernLogicalTypeWinsOverConvertedType() {
        SchemaElement leaf = new SchemaElement(
                Optional.of(PhysicalType.BYTE_ARRAY),
                OptionalInt.empty(),
                Optional.of(FieldRepetitionType.OPTIONAL),
                "col",
                OptionalInt.empty(),
                Optional.of(ConvertedType.ENUM),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                OptionalInt.empty());

        assertThat(build(leaf)).contains(new LogicalType.StringType());
    }

    @Test
    void listConvertedTypeBackfillsListTypeOnGroup() {
        Optional<LogicalType> logicalType = groupLogicalType(ConvertedType.LIST);

        assertThat(logicalType).contains(new LogicalType.ListType());
    }

    @Test
    void mapConvertedTypeBackfillsMapTypeOnGroup() {
        Optional<LogicalType> logicalType = groupLogicalType(ConvertedType.MAP);

        assertThat(logicalType).contains(new LogicalType.MapType());
    }

    @Test
    void mapKeyValueConvertedTypeStaysEmpty() {
        Optional<LogicalType> logicalType = groupLogicalType(ConvertedType.MAP_KEY_VALUE);

        assertThat(logicalType).isEmpty();
    }

    @Test
    void intervalHasNoLogicalEquivalentAndStaysEmpty() {
        Optional<LogicalType> logicalType = leafLogicalType(
                PhysicalType.FIXED_LEN_BYTE_ARRAY, Optional.of(ConvertedType.INTERVAL), Optional.empty());

        assertThat(logicalType).isEmpty();
    }

    @Test
    void leafWithoutAnnotationsStaysEmpty() {
        Optional<LogicalType> logicalType =
                leafLogicalType(PhysicalType.BYTE_ARRAY, Optional.empty(), Optional.empty());

        assertThat(logicalType).isEmpty();
    }

    private static Optional<LogicalType> leafLogicalType(
            PhysicalType physical, Optional<ConvertedType> convertedType, Optional<LogicalType> logicalType) {
        return build(leaf(physical, convertedType, OptionalInt.empty(), OptionalInt.empty(), logicalType));
    }

    private static SchemaElement leaf(
            PhysicalType physical, Optional<ConvertedType> convertedType, OptionalInt scale, OptionalInt precision) {
        return leaf(physical, convertedType, scale, precision, Optional.empty());
    }

    private static SchemaElement leaf(
            PhysicalType physical,
            Optional<ConvertedType> convertedType,
            OptionalInt scale,
            OptionalInt precision,
            Optional<LogicalType> logicalType) {
        return new SchemaElement(
                Optional.of(physical),
                OptionalInt.empty(),
                Optional.of(FieldRepetitionType.OPTIONAL),
                "col",
                OptionalInt.empty(),
                convertedType,
                scale,
                precision,
                logicalType,
                OptionalInt.empty());
    }

    /**
     * The backfilled logical type of a group element annotated with {@code convertedType}. The group holds one leaf
     * child so it is a well-formed group; only the group's own annotation is asserted.
     */
    private static Optional<LogicalType> groupLogicalType(ConvertedType convertedType) {
        SchemaElement root = new SchemaElement(
                Optional.empty(),
                OptionalInt.empty(),
                Optional.of(FieldRepetitionType.REQUIRED),
                "root",
                OptionalInt.of(1),
                Optional.empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty());
        SchemaElement group = new SchemaElement(
                Optional.empty(),
                OptionalInt.empty(),
                Optional.of(FieldRepetitionType.OPTIONAL),
                "group",
                OptionalInt.of(1),
                Optional.of(convertedType),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty());
        SchemaElement child = leaf(PhysicalType.BYTE_ARRAY, Optional.empty(), OptionalInt.empty(), OptionalInt.empty());
        ParquetSchema schema = SchemaBuilder.build(List.of(root, group, child));
        SchemaNode.Group groupNode = (SchemaNode.Group) schema.root().children().get(0);
        return groupNode.logicalType();
    }

    private static Optional<LogicalType> build(SchemaElement leaf) {
        SchemaElement root = new SchemaElement(
                Optional.empty(),
                OptionalInt.empty(),
                Optional.of(FieldRepetitionType.REQUIRED),
                "root",
                OptionalInt.of(1),
                Optional.empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty());
        ParquetSchema schema = SchemaBuilder.build(List.of(root, leaf));
        SchemaNode.Primitive primitive =
                (SchemaNode.Primitive) schema.root().children().get(0);
        return primitive.logicalType();
    }
}
