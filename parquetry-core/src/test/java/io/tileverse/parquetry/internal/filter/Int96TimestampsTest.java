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

import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.time.LocalDateTime;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.filter.Value;

/**
 * Covers the chronological comparison of INT96 cells with timestamp literals, over the values documented for the
 * {@code int96_timestamp_order.parquet} and {@code int96_from_spark.parquet} fixtures of
 * {@code apache/parquet-testing}.
 */
class Int96TimestampsTest {

    static Stream<Arguments> documentedCells() {
        return Stream.of(
                Arguments.of(2440000, 123L, LocalDateTime.parse("1968-05-23T00:00:00.000000123")),
                Arguments.of(2440588, 1000L, LocalDateTime.parse("1970-01-01T00:00:00.000001")),
                Arguments.of(2440588, 86_399_999_999_999L, LocalDateTime.parse("1970-01-01T23:59:59.999999999")),
                Arguments.of(2440589, 0L, LocalDateTime.parse("1970-01-02T00:00")),
                // a Spark value of year 290000, beyond a 64-bit count of nanoseconds since the epoch
                Arguments.of(107_641_749, 82_800_000_000_000L, LocalDateTime.parse("+290000-12-30T23:00")));
    }

    @ParameterizedTest(name = "day {0}, nanos {1} is {2}")
    @MethodSource("documentedCells")
    void cellEqualsItsDocumentedTimestamp(int julianDay, long nanosOfDay, LocalDateTime documented) {
        MemorySegment cell = int96(julianDay, nanosOfDay);

        assertThat(Int96Timestamps.compare(cell, documented)).isZero();
        assertThat(Int96Timestamps.compare(cell, documented.plusNanos(1))).isNegative();
        assertThat(Int96Timestamps.compare(cell, documented.minusNanos(1))).isPositive();
    }

    @Test
    void cellsOrderChronologicallyWhereTheirBytesDoNot() {
        // NEXT_DAY starts with byte 0x00 and LATE_IN_DAY with 0xFF, yet NEXT_DAY is the later of the two.
        MemorySegment nextDay = int96(2440589, 0L);
        LocalDateTime lateInDay = LocalDateTime.parse("1970-01-01T23:59:59.999999999");

        assertThat(Int96Timestamps.compare(nextDay, lateInDay)).isPositive();
    }

    @Test
    void nanosecondsPastTheDayCountAsTheNextDay() {
        MemorySegment unnormalized = int96(2440588, 86_400_000_000_000L + 5L);

        assertThat(Int96Timestamps.compare(unnormalized, LocalDateTime.parse("1970-01-02T00:00:00.000000005")))
                .isZero();
    }

    @Test
    void timestampLiteralComparesAgainstAnInt96CellAtRecordLevel() {
        MemorySegment cell = int96(2440589, 0L);
        Value literal = new Value.TimestampVal(LocalDateTime.parse("1970-01-02T00:00"), true);
        Value earlier = new Value.TimestampVal(LocalDateTime.parse("1970-01-01T12:00"), true);

        assertThat(ValueComparison.holds(ComparisonOperator.EQ, cell, literal)).isTrue();
        assertThat(ValueComparison.holds(ComparisonOperator.EQ, cell, earlier)).isFalse();
        assertThat(ValueComparison.holds(ComparisonOperator.GT, cell, earlier)).isTrue();
    }

    @Test
    void timestampLiteralComparesAgainstAnInt96CellInAColumnScan() {
        MemorySegment cell = int96(2440000, 123L);
        Value later = new Value.TimestampVal(LocalDateTime.parse("1970-01-01T00:00"), true);

        assertThat(ValueComparison.compareBinary(cell, later)).isNegative();
    }

    private static MemorySegment int96(int julianDay, long nanosOfDay) {
        MemorySegment cell = MemorySegment.ofArray(new byte[Int96Timestamps.CELL_BYTES]);
        cell.set(INT64, 0L, nanosOfDay);
        cell.set(INT32, Long.BYTES, julianDay);
        return cell.asReadOnly();
    }
}
