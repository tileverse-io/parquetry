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
package io.tileverse.parquetry.internal.read.page;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.UnsupportedFeatureException;
import io.tileverse.parquetry.internal.read.page.AlpPageBuilder.Vector;
import io.tileverse.parquetry.internal.read.page.AlpPageBuilder.Width;

/**
 * Corrupts a valid ALP page field by field: each inconsistency with the page bytes fails with
 * {@link MalformedFileException}, never with an out-of-bounds access.
 *
 * <p>The baseline page holds twelve values in two vectors of a {@code log_vector_size} of 3. The first vector has eight
 * values packed at four bits and one exception at position 2; the second has four values and no exception.
 */
class AlpPageValidationTest {

    private static final int VALUE_COUNT = 12;
    private static final int LOG_VECTOR_SIZE_OFFSET = 2;
    private static final int ELEMENT_COUNT_OFFSET = 3;
    private static final int FIRST_OFFSET_POSITION = AlpPageBuilder.PAGE_HEADER_BYTES;
    private static final int SECOND_OFFSET_POSITION = FIRST_OFFSET_POSITION + Integer.BYTES;
    private static final int FIRST_VECTOR = SECOND_OFFSET_POSITION + Integer.BYTES;
    private static final int EXPONENT = 0;
    private static final int FACTOR = 1;
    private static final int EXCEPTION_COUNT = 2;
    private static final int FRAME_OF_REFERENCE = 4;

    static Stream<Arguments> corruptions() {
        return Stream.of(
                corruption("page shorter than its header", Width.DOUBLE, truncatedTo(5)),
                corruption("log_vector_size below 3", Width.DOUBLE, byteAt(LOG_VECTOR_SIZE_OFFSET, 2)),
                corruption("log_vector_size above 15", Width.DOUBLE, byteAt(LOG_VECTOR_SIZE_OFFSET, 16)),
                corruption("negative element count", Width.DOUBLE, intAt(ELEMENT_COUNT_OFFSET, -1)),
                corruption("more vector offsets than page bytes", Width.DOUBLE, intAt(ELEMENT_COUNT_OFFSET, 1000)),
                corruption("vector offset into the offset array", Width.DOUBLE, intAt(FIRST_OFFSET_POSITION, 4)),
                corruption("vector offset past the page end", Width.DOUBLE, intAt(SECOND_OFFSET_POSITION, 100_000)),
                corruption("double exponent above 18", Width.DOUBLE, byteAt(FIRST_VECTOR + EXPONENT, 19)),
                corruption("float exponent above 10", Width.FLOAT, byteAt(FIRST_VECTOR + EXPONENT, 11)),
                corruption("factor above the exponent", Width.DOUBLE, byteAt(FIRST_VECTOR + FACTOR, 2)),
                corruption("double bit width above 64", Width.DOUBLE, byteAt(bitWidthOffset(Width.DOUBLE), 65)),
                corruption("float bit width above 32", Width.FLOAT, byteAt(bitWidthOffset(Width.FLOAT), 33)),
                corruption("more exceptions than values", Width.DOUBLE, shortAt(FIRST_VECTOR + EXCEPTION_COUNT, 9)),
                corruption("exceptions overrunning the page", Width.DOUBLE, shortAt(FIRST_VECTOR + EXCEPTION_COUNT, 8)),
                corruption(
                        "exception position outside the vector",
                        Width.DOUBLE,
                        shortAt(firstExceptionPositionOffset(Width.DOUBLE), 8)),
                corruption("vector data cut short", Width.DOUBLE, truncatedBy(1)),
                corruption("float vector data cut short", Width.FLOAT, truncatedBy(1)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("corruptions")
    void inconsistentPageFailsAsMalformed(String description, Width width, PageEdit edit) {
        byte[] page = edit.apply(baselinePage(width));

        assertThatThrownBy(() -> decodeAll(width, page)).isInstanceOf(MalformedFileException.class);
    }

    @Test
    void compressionModeOtherThanAlpIsUnsupported() {
        byte[] page = baselinePage(Width.DOUBLE);
        page[0] = 1;

        assertThatThrownBy(() -> decodeAll(Width.DOUBLE, page))
                .isInstanceOf(UnsupportedFeatureException.class)
                .hasMessageContaining("compression mode 1");
    }

    @Test
    void integerEncodingOtherThanFrameOfReferenceIsUnsupported() {
        byte[] page = baselinePage(Width.FLOAT);
        page[1] = 1;

        assertThatThrownBy(() -> decodeAll(Width.FLOAT, page))
                .isInstanceOf(UnsupportedFeatureException.class)
                .hasMessageContaining("integer encoding 1");
    }

    /** A change applied to a copy of the baseline page bytes. */
    @FunctionalInterface
    interface PageEdit {
        byte[] apply(byte[] page);
    }

    private static Arguments corruption(String description, Width width, PageEdit edit) {
        return Arguments.of(description, width, edit);
    }

    private static byte[] baselinePage(Width width) {
        long exceptionBits =
                width == Width.FLOAT ? Float.floatToRawIntBits(Float.NaN) : Double.doubleToRawLongBits(Double.NaN);
        Vector first = new Vector(
                1, 0, 10L, 4, new long[] {0, 1, 0, 3, 4, 5, 6, 15}, new int[] {2}, new long[] {exceptionBits});
        Vector second = Vector.withoutExceptions(2, 1, -4L, 2, 0, 1, 2, 3);
        return AlpPageBuilder.page(width, 3, List.of(first, second));
    }

    private static int bitWidthOffset(Width width) {
        return FIRST_VECTOR + FRAME_OF_REFERENCE + width.valueBytes();
    }

    /** The first vector's eight four-bit deltas take four bytes after its header. */
    private static int firstExceptionPositionOffset(Width width) {
        return bitWidthOffset(width) + 1 + 4;
    }

    private static void decodeAll(Width width, byte[] page) {
        MemorySegment segment = MemorySegment.ofArray(page);
        if (width == Width.FLOAT) {
            AlpFloatDecoder decoder = new AlpFloatDecoder();
            decoder.load(segment, VALUE_COUNT);
            decoder.decodeFloats(VALUE_COUNT, new float[VALUE_COUNT], 0);
        } else {
            AlpDoubleDecoder decoder = new AlpDoubleDecoder();
            decoder.load(segment, VALUE_COUNT);
            decoder.decodeDoubles(VALUE_COUNT, new double[VALUE_COUNT], 0);
        }
    }

    private static PageEdit truncatedTo(int length) {
        return page -> Arrays.copyOf(page, length);
    }

    private static PageEdit truncatedBy(int bytes) {
        return page -> Arrays.copyOf(page, page.length - bytes);
    }

    private static PageEdit byteAt(int offset, int value) {
        return littleEndian(buffer -> buffer.put(offset, (byte) value));
    }

    private static PageEdit shortAt(int offset, int value) {
        return littleEndian(buffer -> buffer.putShort(offset, (short) value));
    }

    private static PageEdit intAt(int offset, int value) {
        return littleEndian(buffer -> buffer.putInt(offset, value));
    }

    private static PageEdit littleEndian(Consumer<ByteBuffer> change) {
        return page -> {
            byte[] edited = page.clone();
            change.accept(ByteBuffer.wrap(edited).order(ByteOrder.LITTLE_ENDIAN));
            return edited;
        };
    }
}
