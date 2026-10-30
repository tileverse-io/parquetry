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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Covers what a decoded binary bound owns. Callers keep a bound far longer than the memory that it was decoded from - a
 * catalog holds one min and one max per column for the lifetime of a store, while the footer behind them is cached and
 * evicted on its own schedule - and every bound therefore holds memory of its own, sized to the value.
 */
class StatisticsValueDecoderTest {

    private static final byte[] VALUE = "omega".getBytes(StandardCharsets.UTF_8);

    /** Padding around the value, giving an aliasing decode a wider source region to point into. */
    private static final int LEADING_BYTES = 7;

    private static final int TRAILING_BYTES = 11;

    /** The two physical kinds decoding to a binary bound; a decimal FLBA decodes to a number instead. */
    private static Stream<PrimitiveKind> binaryKinds() {
        return Stream.of(PrimitiveKind.BYTE_ARRAY, PrimitiveKind.FIXED_LEN_BYTE_ARRAY);
    }

    @ParameterizedTest
    @MethodSource("binaryKinds")
    void decodedBoundOwnsExactlyTheValueBytes(PrimitiveKind kind) {
        byte[] source = sourceHoldingTheValue();
        MemorySegment whole = MemorySegment.ofArray(source);
        MemorySegment raw = whole.asSlice(LEADING_BYTES, VALUE.length);

        MemorySegment decoded = decodeBinaryBound(kind, raw);

        assertThat(decoded.byteSize()).as("bytes of the decoded bound").isEqualTo(VALUE.length);
        assertThat(decoded.toArray(JAVA_BYTE))
                .as("content of the decoded bound")
                .isEqualTo(VALUE);
        // A read-only segment declines to name its backing array, hence the overlap check rather than a comparison of
        // heap bases: a window into the source reports the source region as shared, a copy of its own reports nothing.
        assertThat(whole.asOverlappingSlice(decoded))
                .as("region of the source shared with the decoded bound")
                .isEmpty();
    }

    @ParameterizedTest
    @MethodSource("binaryKinds")
    void decodedBoundKeepsItsBytesWhenTheSourceIsOverwritten(PrimitiveKind kind) {
        byte[] source = sourceHoldingTheValue();
        MemorySegment raw = MemorySegment.ofArray(source).asSlice(LEADING_BYTES, VALUE.length);
        MemorySegment decoded = decodeBinaryBound(kind, raw);

        Arrays.fill(source, (byte) 0);

        assertThat(decoded.toArray(JAVA_BYTE))
                .as("content of the decoded bound after the source was overwritten")
                .isEqualTo(VALUE);
    }

    /**
     * A time bound outside one day proves nothing and must not fail the read. The last value overflows to 384
     * nanoseconds when scaled to nanoseconds: a wrong bound inside the day.
     */
    @ParameterizedTest
    @ValueSource(longs = {-1L, 86_400_000_000L, 18_446_744_073_709_552L})
    void timeBoundOutsideADayIsDropped(long micros) {
        MemorySegment raw = MemorySegment.ofArray(ByteBuffer.allocate(Long.BYTES)
                .order(LITTLE_ENDIAN)
                .putLong(micros)
                .array());
        LogicalType time = new LogicalType.Time(false, LogicalType.TimeUnit.MICROS);

        Optional<Value> decoded = StatisticsValueDecoder.decode(PrimitiveKind.INT64, Optional.of(time), raw);

        assertThat(decoded).isEmpty();
    }

    /** The binary bound decoded from {@code raw}, failing the test when the decoder produces another shape. */
    private static MemorySegment decodeBinaryBound(PrimitiveKind kind, MemorySegment raw) {
        Optional<Value> decoded = StatisticsValueDecoder.decode(kind, Optional.empty(), raw);
        assertThat(decoded).get().isInstanceOf(Value.BinaryVal.class);
        Value.BinaryVal binary = (Value.BinaryVal) decoded.orElseThrow();
        return binary.value();
    }

    /** A fresh array holding {@link #VALUE} between two runs of filler bytes. */
    private static byte[] sourceHoldingTheValue() {
        byte[] source = new byte[LEADING_BYTES + VALUE.length + TRAILING_BYTES];
        Arrays.fill(source, (byte) 0x7A);
        System.arraycopy(VALUE, 0, source, LEADING_BYTES, VALUE.length);
        return source;
    }
}
