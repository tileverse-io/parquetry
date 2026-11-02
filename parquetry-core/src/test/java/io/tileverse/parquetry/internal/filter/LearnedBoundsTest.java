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

import static io.tileverse.parquetry.format.ParquetLayouts.INT64;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.schema.PrimitiveKind;

class LearnedBoundsTest {

    private static final Optional<LogicalType> NO_ANNOTATION = Optional.empty();

    @Test
    void minValueIsPreferred() {
        Statistics statistics =
                Statistics.builder().min(int64(1)).minValue(int64(2)).build();

        Optional<MemorySegment> min = LearnedBounds.min(statistics, PrimitiveKind.INT64, NO_ANNOTATION);

        assertThat(min)
                .hasValueSatisfying(bound -> assertThat(bound.get(INT64, 0)).isEqualTo(2L));
    }

    // The writers of the deprecated fields compared binary values as signed bytes.
    @Test
    void deprecatedMinOnlyForNumbers() {
        Statistics number = Statistics.builder().min(int64(7)).build();
        Statistics binary = Statistics.builder().min(ascii("Bern")).build();

        Optional<MemorySegment> numberMin = LearnedBounds.min(number, PrimitiveKind.INT64, NO_ANNOTATION);
        Optional<MemorySegment> binaryMin = LearnedBounds.min(binary, PrimitiveKind.BYTE_ARRAY, NO_ANNOTATION);

        assertThat(numberMin)
                .hasValueSatisfying(bound -> assertThat(bound.get(INT64, 0)).isEqualTo(7L));
        assertThat(binaryMin).isEmpty();
    }

    @Test
    void geometryGetsNoBounds() {
        Statistics statistics = Statistics.builder()
                .minValue(filled(21, 0x01))
                .maxValue(filled(21, 0x02))
                .build();
        Optional<LogicalType> geometry = Optional.of(new LogicalType.Geometry(Optional.empty()));

        assertThat(LearnedBounds.min(statistics, PrimitiveKind.BYTE_ARRAY, geometry))
                .isEmpty();
        assertThat(LearnedBounds.max(statistics, PrimitiveKind.BYTE_ARRAY, geometry))
                .isEmpty();
    }

    @Test
    void int96GetsNoBounds() {
        Statistics statistics = Statistics.builder()
                .minValue(filled(12, 0x01))
                .maxValue(filled(12, 0x02))
                .build();

        assertThat(LearnedBounds.min(statistics, PrimitiveKind.INT96, NO_ANNOTATION))
                .isEmpty();
        assertThat(LearnedBounds.max(statistics, PrimitiveKind.INT96, NO_ANNOTATION))
                .isEmpty();
    }

    @Test
    void longMinIsCutToAPrefix() {
        byte[] longMin = "abcdefghij".repeat(10).getBytes(StandardCharsets.US_ASCII);

        byte[] kept = keptMin(longMin);

        assertThat(kept).isEqualTo(Arrays.copyOf(longMin, LearnedBounds.MAX_BINARY_BOUND_BYTES));
    }

    // The raised byte puts the cut maximum above the values starting with the bytes before it.
    @Test
    void longMaxIsRaisedAtItsLastByte() {
        byte[] letters = "a".repeat(100).getBytes(StandardCharsets.US_ASCII);
        byte[] lettersThenFf = Arrays.copyOf(letters, letters.length);
        Arrays.fill(lettersThenFf, 60, 100, (byte) 0xFF);

        byte[] keptLetters = keptMax(letters);
        byte[] keptLettersThenFf = keptMax(lettersThenFf);

        assertThat(keptLetters).isEqualTo(("a".repeat(63) + "b").getBytes(StandardCharsets.US_ASCII));
        assertThat(keptLettersThenFf)
                .as("the 0xFF bytes after the raised byte are cut")
                .isEqualTo(("a".repeat(59) + "b").getBytes(StandardCharsets.US_ASCII));
    }

    // No prefix of 0xFF bytes can be raised above the maximum.
    @Test
    void allFfMaxStaysWhole() {
        byte[] allFf = new byte[100];
        Arrays.fill(allFf, (byte) 0xFF);

        byte[] kept = keptMax(allFf);

        assertThat(kept).isEqualTo(allFf);
    }

    // A decimal compares as a number, and a prefix of its bytes is another number.
    @Test
    void decimalFixedBytesStayWhole() {
        byte[] longDecimal = "0123456789".repeat(10).getBytes(StandardCharsets.US_ASCII);
        MemorySegment bound = MemorySegment.ofArray(longDecimal);
        Statistics statistics =
                Statistics.builder().minValue(bound).maxValue(bound).build();
        Optional<LogicalType> decimal = Optional.of(new LogicalType.Decimal(2, 38));
        PrimitiveKind fixed = PrimitiveKind.FIXED_LEN_BYTE_ARRAY;

        MemorySegment min = LearnedBounds.min(statistics, fixed, decimal).orElseThrow();
        MemorySegment max = LearnedBounds.max(statistics, fixed, decimal).orElseThrow();

        assertThat(min.toArray(JAVA_BYTE)).isEqualTo(longDecimal);
        assertThat(max.toArray(JAVA_BYTE)).isEqualTo(longDecimal);
    }

    // The decoder reads no number from an empty decimal.
    @Test
    void emptyDecimalIsDropped() {
        Statistics statistics = Statistics.builder()
                .minValue(filled(0, 0))
                .maxValue(filled(1, 0x01))
                .build();
        Optional<LogicalType> decimal = Optional.of(new LogicalType.Decimal(2, 9));
        PrimitiveKind fixed = PrimitiveKind.FIXED_LEN_BYTE_ARRAY;

        assertThat(LearnedBounds.min(statistics, fixed, decimal)).isEmpty();
        assertThat(LearnedBounds.max(statistics, fixed, decimal)).isPresent();
    }

    static Stream<Arguments> undecodedTypes() {
        return Stream.of(
                Arguments.of(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, new LogicalType.Float16Type()),
                Arguments.of(PrimitiveKind.BYTE_ARRAY, new LogicalType.Decimal(2, 9)),
                Arguments.of(PrimitiveKind.INT64, new LogicalType.IntType((byte) 64, false)),
                Arguments.of(PrimitiveKind.INT32, new LogicalType.UnknownType()),
                Arguments.of(PrimitiveKind.BYTE_ARRAY, new LogicalType.Geography(Optional.empty(), Optional.empty())));
    }

    // The page view decodes no bound of these types.
    @ParameterizedTest(name = "{0} annotated {1}")
    @MethodSource("undecodedTypes")
    void undecodedTypesGetNoBounds(PrimitiveKind kind, LogicalType logicalType) {
        Statistics statistics = Statistics.builder()
                .minValue(filled(8, 0x01))
                .maxValue(filled(8, 0x02))
                .build();

        assertThat(LearnedBounds.min(statistics, kind, Optional.of(logicalType)))
                .isEmpty();
        assertThat(LearnedBounds.max(statistics, kind, Optional.of(logicalType)))
                .isEmpty();
    }

    // The decoder reads one value from the first bytes of a bound, and none from a shorter bound.
    @Test
    void numberBoundsKeepTheirWidth() {
        Statistics tooShort = Statistics.builder().minValue(filled(3, 0x01)).build();
        Statistics tooLong = Statistics.builder().minValue(filled(6, 0x01)).build();

        Optional<MemorySegment> shortMin = LearnedBounds.min(tooShort, PrimitiveKind.INT32, NO_ANNOTATION);
        Optional<MemorySegment> longMin = LearnedBounds.min(tooLong, PrimitiveKind.INT32, NO_ANNOTATION);

        assertThat(shortMin).isEmpty();
        assertThat(longMin)
                .hasValueSatisfying(bound -> assertThat(bound.byteSize()).isEqualTo(Integer.BYTES));
    }

    // A page header read from a pooled buffer is released after the scan, and its bounds must stay readable.
    @Test
    void boundsOutliveTheirSource() {
        Optional<MemorySegment> min;
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment source = arena.allocate(Long.BYTES);
            source.set(INT64, 0, 42L);
            Statistics statistics = Statistics.builder().minValue(source).build();
            min = LearnedBounds.min(statistics, PrimitiveKind.INT64, NO_ANNOTATION);
        }

        assertThat(min)
                .hasValueSatisfying(bound -> assertThat(bound.get(INT64, 0)).isEqualTo(42L));
    }

    private static byte[] keptMin(byte[] min) {
        Statistics statistics =
                Statistics.builder().minValue(MemorySegment.ofArray(min)).build();
        MemorySegment kept = LearnedBounds.min(statistics, PrimitiveKind.BYTE_ARRAY, NO_ANNOTATION)
                .orElseThrow();
        return kept.toArray(JAVA_BYTE);
    }

    private static byte[] keptMax(byte[] max) {
        Statistics statistics =
                Statistics.builder().maxValue(MemorySegment.ofArray(max)).build();
        MemorySegment kept = LearnedBounds.max(statistics, PrimitiveKind.BYTE_ARRAY, NO_ANNOTATION)
                .orElseThrow();
        return kept.toArray(JAVA_BYTE);
    }

    private static MemorySegment int64(long value) {
        MemorySegment segment = MemorySegment.ofArray(new byte[Long.BYTES]);
        segment.set(INT64, 0, value);
        return segment;
    }

    private static MemorySegment ascii(String text) {
        return MemorySegment.ofArray(text.getBytes(StandardCharsets.US_ASCII));
    }

    private static MemorySegment filled(int length, int value) {
        byte[] bytes = new byte[length];
        Arrays.fill(bytes, (byte) value);
        return MemorySegment.ofArray(bytes);
    }
}
