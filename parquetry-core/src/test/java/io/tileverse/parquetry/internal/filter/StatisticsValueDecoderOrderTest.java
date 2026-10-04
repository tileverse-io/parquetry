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
package io.tileverse.parquetry.internal.filter;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Covers the statistics bounds handed by the decoder to the pruning tiers. A bound ordered unlike the way the filter
 * compares the column's cells would let a tier drop matching rows; the decoder declines it and the column takes no part
 * in min/max pruning.
 */
class StatisticsValueDecoderOrderTest {

    private static final MemorySegment TWELVE_BYTES = MemorySegment.ofArray(new byte[12]);

    static Stream<Arguments> boundsOrderedUnlikeTheirCells() {
        return Stream.of(
                Arguments.of(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, new LogicalType.Float16Type()),
                Arguments.of(
                        PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                        new LogicalType.Timestamp(true, LogicalType.TimeUnit.MILLIS)),
                Arguments.of(PrimitiveKind.BYTE_ARRAY, new LogicalType.Decimal(2, 9)),
                Arguments.of(PrimitiveKind.INT32, new LogicalType.UuidType()),
                Arguments.of(PrimitiveKind.INT32, new LogicalType.IntType((byte) 32, false)),
                Arguments.of(PrimitiveKind.INT64, new LogicalType.IntType((byte) 64, false)),
                Arguments.of(PrimitiveKind.BYTE_ARRAY, new LogicalType.Geometry(Optional.empty())),
                Arguments.of(PrimitiveKind.BYTE_ARRAY, new LogicalType.Geography(Optional.empty(), Optional.empty())),
                Arguments.of(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, new LogicalType.StringType()),
                Arguments.of(PrimitiveKind.INT64, new LogicalType.DateType()));
    }

    @ParameterizedTest(name = "{0} annotated {1}")
    @MethodSource("boundsOrderedUnlikeTheirCells")
    void boundOrderedUnlikeItsCellsIsDeclined(PrimitiveKind kind, LogicalType logicalType) {
        Optional<Value> decoded = StatisticsValueDecoder.decode(kind, Optional.of(logicalType), TWELVE_BYTES);

        assertThat(decoded).isEmpty();
    }

    static Stream<Arguments> boundsOrderedLikeTheirCells() {
        return Stream.of(
                Arguments.of(PrimitiveKind.INT32, new LogicalType.IntType((byte) 16, false)),
                Arguments.of(PrimitiveKind.INT32, new LogicalType.IntType((byte) 32, true)),
                Arguments.of(PrimitiveKind.INT64, new LogicalType.IntType((byte) 32, false)),
                Arguments.of(PrimitiveKind.INT32, new LogicalType.DateType()),
                Arguments.of(PrimitiveKind.INT64, new LogicalType.Timestamp(true, LogicalType.TimeUnit.MICROS)),
                Arguments.of(PrimitiveKind.BYTE_ARRAY, new LogicalType.StringType()),
                Arguments.of(PrimitiveKind.BYTE_ARRAY, new LogicalType.JsonType()),
                Arguments.of(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, new LogicalType.UuidType()),
                Arguments.of(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, new LogicalType.Decimal(2, 20)),
                Arguments.of(PrimitiveKind.INT32, new LogicalType.Decimal(2, 9)),
                Arguments.of(PrimitiveKind.INT64, new LogicalType.Decimal(2, 18)));
    }

    @ParameterizedTest(name = "{0} annotated {1}")
    @MethodSource("boundsOrderedLikeTheirCells")
    void boundOrderedLikeItsCellsDecodes(PrimitiveKind kind, LogicalType logicalType) {
        Optional<Value> decoded = StatisticsValueDecoder.decode(kind, Optional.of(logicalType), TWELVE_BYTES);

        assertThat(decoded).isPresent();
    }

    @Test
    void fixedLengthDecimalDecodesToItsSignedValue() {
        MemorySegment minusOneHundredth = MemorySegment.ofArray(new byte[] {(byte) 0xFF, (byte) 0xFF});

        Optional<Value> decoded = StatisticsValueDecoder.decode(
                PrimitiveKind.FIXED_LEN_BYTE_ARRAY, Optional.of(new LogicalType.Decimal(2, 4)), minusOneHundredth);

        assertThat(decoded).contains(new Value.DecimalVal(new BigDecimal("-0.01")));
    }
}
