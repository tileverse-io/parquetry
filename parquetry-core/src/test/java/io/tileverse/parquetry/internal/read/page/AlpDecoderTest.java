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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.ParquetLayouts;
import io.tileverse.parquetry.internal.read.page.AlpPageBuilder.Vector;
import io.tileverse.parquetry.internal.read.page.AlpPageBuilder.Width;

class AlpDecoderTest {

    private static final float FLOAT_NAN_WITH_PAYLOAD = Float.intBitsToFloat(0x7FC0DEAD);
    private static final float NEGATIVE_FLOAT_NAN_WITH_PAYLOAD = Float.intBitsToFloat(0xFFC00001);
    private static final double DOUBLE_NAN_WITH_PAYLOAD = Double.longBitsToDouble(0x7FF800DEADBEEF00L);
    private static final double NEGATIVE_DOUBLE_NAN_WITH_PAYLOAD = Double.longBitsToDouble(0xFFF8000000000001L);

    @Test
    void workedExampleOfTheSpecificationDecodesToItsInput() {
        long nanBits = Double.doubleToRawLongBits(Double.NaN);
        Vector vector =
                new Vector(4, 3, 3335L, 15, new long[] {11665, 11665, 21665, 0}, new int[] {1}, new long[] {nanBits});
        byte[] page = AlpPageBuilder.page(Width.DOUBLE, 10, List.of(vector));

        assertThat(vector.byteSize(Width.DOUBLE)).isEqualTo(31);
        assertThat(bitsOf(decodeDoubles(page, 4))).containsExactly(bitsOf(1500.0, Double.NaN, 2500.0, 333.5));
    }

    @Test
    void frameOfReferenceExampleOfTheSpecificationUnpacksTenBitDeltas() {
        Vector vector = Vector.withoutExceptions(0, 0, 12L, 10, 111, 444, 777, 0);

        byte[] floatPage = AlpPageBuilder.page(Width.FLOAT, 10, List.of(vector));
        byte[] doublePage = AlpPageBuilder.page(Width.DOUBLE, 10, List.of(vector));

        assertThat(AlpPageBuilder.pack(vector.deltas(), 10)).hasSize(5);
        assertThat(decodeFloats(floatPage, 4)).containsExactly(123f, 456f, 789f, 12f);
        assertThat(decodeDoubles(doublePage, 4)).containsExactly(123.0, 456.0, 789.0, 12.0);
    }

    @Test
    void bitWidthZeroVectorRepeatsItsValue() {
        float[] floats = new float[8];
        double[] doubles = new double[8];
        Arrays.fill(floats, 7.77f);
        Arrays.fill(doubles, 7.77);

        assertThat(AlpPageBuilder.encodeFloats(2, 0, floats).bitWidth()).isZero();
        assertThat(AlpPageBuilder.encodeDoubles(2, 0, doubles).bitWidth()).isZero();
        assertThat(decodeFloats(AlpPageBuilder.floatPage(3, 2, 0, floats), 8)).containsExactly(floats);
        assertThat(decodeDoubles(AlpPageBuilder.doublePage(3, 2, 0, doubles), 8))
                .containsExactly(doubles);
    }

    @Test
    void floatExceptionsKeepTheirStoredBits() {
        float[] values = {
            1.25f,
            FLOAT_NAN_WITH_PAYLOAD,
            Float.POSITIVE_INFINITY,
            Float.NEGATIVE_INFINITY,
            -0.0f,
            Float.MIN_VALUE,
            NEGATIVE_FLOAT_NAN_WITH_PAYLOAD,
            (float) Math.PI,
            2.5f
        };
        byte[] page = AlpPageBuilder.floatPage(10, 2, 0, values);

        assertThat(AlpPageBuilder.encodeFloats(2, 0, values).exceptionCount()).isEqualTo(7);
        assertThat(bitsOf(decodeFloats(page, values.length))).containsExactly(bitsOf(values));
        assertThat(bitsOf(decodeFloatsIntoSegment(page, values.length))).containsExactly(bitsOf(values));
    }

    @Test
    void doubleExceptionsKeepTheirStoredBits() {
        double[] values = {
            1.25,
            DOUBLE_NAN_WITH_PAYLOAD,
            Double.POSITIVE_INFINITY,
            Double.NEGATIVE_INFINITY,
            -0.0,
            Double.MIN_VALUE,
            NEGATIVE_DOUBLE_NAN_WITH_PAYLOAD,
            Math.PI,
            2.5
        };
        byte[] page = AlpPageBuilder.doublePage(10, 2, 0, values);

        assertThat(AlpPageBuilder.encodeDoubles(2, 0, values).exceptionCount()).isEqualTo(7);
        assertThat(bitsOf(decodeDoubles(page, values.length))).containsExactly(bitsOf(values));
        assertThat(bitsOf(decodeDoublesIntoSegment(page, values.length))).containsExactly(bitsOf(values));
    }

    @Test
    void allExceptionVectorDecodesFromItsStoredValues() {
        double[] values = new Random(42).doubles(1024).toArray();

        Vector vector = AlpPageBuilder.encodeDoubles(0, 0, values);
        byte[] page = AlpPageBuilder.page(Width.DOUBLE, 10, List.of(vector));

        assertThat(vector.exceptionCount()).isEqualTo(values.length);
        assertThat(vector.bitWidth()).isZero();
        assertThat(bitsOf(decodeDoubles(page, values.length))).containsExactly(bitsOf(values));
    }

    @Test
    void exceptionsListedOutOfPositionOrderArePatched() {
        long[] exceptionBits = {bitsOf(-1.5)[0], bitsOf(Double.NaN)[0], bitsOf(9.25)[0]};
        Vector vector = new Vector(0, 0, 5L, 0, new long[4], new int[] {3, 0, 2}, exceptionBits);
        byte[] page = AlpPageBuilder.page(Width.DOUBLE, 10, List.of(vector));
        long[] expected = bitsOf(Double.NaN, 5.0, 9.25, -1.5);

        assertThat(bitsOf(decodeDoubles(page, 4))).containsExactly(expected);
        assertThat(bitsOf(decodeDoublesOneByOne(page, 4))).containsExactly(expected);
    }

    @Test
    void sixtyFourBitDeltasWrapAroundTheFrameOfReference() {
        long frame = -8_000_000_000_000_000_000L;
        long sixteenQuintillion = Long.parseUnsignedLong("16000000000000000000");
        Vector vector = Vector.withoutExceptions(0, 0, frame, 64, 0L, sixteenQuintillion, 8_000_000_000_000_000_000L);
        byte[] page = AlpPageBuilder.page(Width.DOUBLE, 10, List.of(vector));

        assertThat(decodeDoubles(page, 3)).containsExactly(-8e18, 8e18, 0.0);
    }

    @Test
    void deltasStraddlingNineBytesUnpack() {
        long[] deltas = new Random(7).longs(40, 0, 1L << 61).toArray();
        Vector vector = Vector.withoutExceptions(0, 0, 0L, 61, deltas);
        byte[] page = AlpPageBuilder.page(Width.DOUBLE, 10, List.of(vector));
        double[] expected = Arrays.stream(deltas).asDoubleStream().toArray();

        assertThat(decodeDoubles(page, deltas.length)).containsExactly(expected);
    }

    @Test
    void floatDeltasUseUnsignedThirtyOneBitsAboveANegativeFrame() {
        long[] deltas = {0L, (1L << 31) - 1, 1L << 30, 12345L};
        Vector vector = Vector.withoutExceptions(0, 0, -(1L << 30), 31, deltas);
        byte[] page = AlpPageBuilder.page(Width.FLOAT, 10, List.of(vector));

        assertThat(decodeFloats(page, 4))
                .containsExactly((float) -(1 << 30), (float) ((1 << 30) - 1), 0f, (float) (12345 - (1 << 30)));
    }

    @Test
    void vectorsFollowTheLogVectorSizeOfThePageHeader() {
        double[] values = decimals(new Random(3), 21, 100);
        List<Vector> vectors = List.of(
                AlpPageBuilder.encodeDoubles(2, 0, Arrays.copyOfRange(values, 0, 8)),
                AlpPageBuilder.encodeDoubles(5, 3, Arrays.copyOfRange(values, 8, 16)),
                AlpPageBuilder.encodeDoubles(0, 0, Arrays.copyOfRange(values, 16, 21)));
        byte[] page = AlpPageBuilder.page(Width.DOUBLE, 3, vectors);

        assertThat(bitsOf(decodeDoubles(page, values.length))).containsExactly(bitsOf(values));
    }

    static Stream<Integer> logVectorSizes() {
        return Stream.of(3, 5, 10, 12, 15);
    }

    @ParameterizedTest(name = "log_vector_size {0}")
    @MethodSource("logVectorSizes")
    void decimalDataRoundTripsAtEachVectorSize(int logVectorSize) {
        Random random = new Random(logVectorSize);
        double[] doubles = withSpecialValues(decimals(random, 5000, 100));
        float[] floats = toFloats(doubles);

        byte[] doublePage = AlpPageBuilder.doublePage(logVectorSize, 2, 0, doubles);
        byte[] floatPage = AlpPageBuilder.floatPage(logVectorSize, 2, 0, floats);

        assertThat(bitsOf(decodeDoubles(doublePage, doubles.length))).containsExactly(bitsOf(doubles));
        assertThat(bitsOf(decodeDoublesIntoSegment(doublePage, doubles.length))).containsExactly(bitsOf(doubles));
        assertThat(bitsOf(decodeFloats(floatPage, floats.length))).containsExactly(bitsOf(floats));
        assertThat(bitsOf(decodeFloatsIntoSegment(floatPage, floats.length))).containsExactly(bitsOf(floats));
    }

    @Test
    void runsAndSkipsAcrossVectorBoundariesMatchTheFullDecode() {
        double[] values = withSpecialValues(decimals(new Random(11), 50, 1000));
        byte[] page = AlpPageBuilder.doublePage(3, 3, 0, values);
        long[] expected = bitsOf(values);
        AlpDoubleDecoder decoder = new AlpDoubleDecoder();
        decoder.load(MemorySegment.ofArray(page), values.length);
        double[] firstRun = new double[7];
        double[] lastValues = new double[25];

        decoder.skip(3);
        decoder.decodeDoubles(7, firstRun, 0);
        double single = decoder.next();
        decoder.skip(13);
        MemorySegment segment = MemorySegment.ofArray(new double[2]);
        decoder.decodeDoubles(1, segment, 1L);
        decoder.decodeDoubles(25, lastValues, 0);

        assertThat(bitsOf(firstRun)).containsExactly(Arrays.copyOfRange(expected, 3, 10));
        assertThat(bitsOf(single)).containsExactly(expected[10]);
        assertThat(segment.getAtIndex(ParquetLayouts.INT64, 1L)).isEqualTo(expected[24]);
        assertThat(bitsOf(lastValues)).containsExactly(Arrays.copyOfRange(expected, 25, 50));
    }

    @Test
    void readingPastTheLastValueFails() {
        byte[] page = AlpPageBuilder.doublePage(10, 0, 0, 1.0, 2.0, 3.0);
        AlpDoubleDecoder decoder = new AlpDoubleDecoder();
        decoder.load(MemorySegment.ofArray(page), 3);

        assertThatThrownBy(() -> decoder.decodeDoubles(4, new double[4], 0))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("holding 3 values");
        assertThatThrownBy(() -> decoder.skip(4)).isInstanceOf(MalformedFileException.class);
    }

    private static double[] decodeDoubles(byte[] page, int count) {
        AlpDoubleDecoder decoder = new AlpDoubleDecoder();
        decoder.load(MemorySegment.ofArray(page), count);
        double[] values = new double[count];
        decoder.decodeDoubles(count, values, 0);
        return values;
    }

    private static double[] decodeDoublesOneByOne(byte[] page, int count) {
        AlpDoubleDecoder decoder = new AlpDoubleDecoder();
        decoder.load(MemorySegment.ofArray(page), count);
        double[] values = new double[count];
        for (int i = 0; i < count; i++) {
            values[i] = decoder.next();
        }
        return values;
    }

    private static double[] decodeDoublesIntoSegment(byte[] page, int count) {
        AlpDoubleDecoder decoder = new AlpDoubleDecoder();
        decoder.load(MemorySegment.ofArray(page), count);
        MemorySegment values = MemorySegment.ofArray(new byte[count * Double.BYTES]);
        decoder.decodeDoubles(count, values, 0L);
        double[] decoded = new double[count];
        for (int i = 0; i < count; i++) {
            decoded[i] = Double.longBitsToDouble(values.getAtIndex(ParquetLayouts.INT64, i));
        }
        return decoded;
    }

    private static float[] decodeFloats(byte[] page, int count) {
        AlpFloatDecoder decoder = new AlpFloatDecoder();
        decoder.load(MemorySegment.ofArray(page), count);
        float[] values = new float[count];
        decoder.decodeFloats(count, values, 0);
        return values;
    }

    private static float[] decodeFloatsIntoSegment(byte[] page, int count) {
        AlpFloatDecoder decoder = new AlpFloatDecoder();
        decoder.load(MemorySegment.ofArray(page), count);
        MemorySegment values = MemorySegment.ofArray(new byte[count * Float.BYTES]);
        decoder.decodeFloats(count, values, 0L);
        float[] decoded = new float[count];
        for (int i = 0; i < count; i++) {
            decoded[i] = Float.intBitsToFloat(values.getAtIndex(ParquetLayouts.INT32, i));
        }
        return decoded;
    }

    /**
     * Random decimals in [-10, 10] with {@code 1 / scale} as their last digit. Dividing two exact integers rounds once,
     * yielding the double nearest to each decimal.
     */
    private static double[] decimals(Random random, int count, int scale) {
        double[] values = new double[count];
        for (int i = 0; i < count; i++) {
            long scaled = random.nextLong(-10L * scale, 10L * scale + 1);
            values[i] = scaled / (double) scale;
        }
        return values;
    }

    /**
     * Sprinkles exceptions over the data: NaN payloads, infinities, negative zero, subnormals, full-mantissa values.
     */
    private static double[] withSpecialValues(double[] values) {
        double[] specials = {
            DOUBLE_NAN_WITH_PAYLOAD,
            Double.POSITIVE_INFINITY,
            -0.0,
            Double.MIN_VALUE,
            Math.PI,
            NEGATIVE_DOUBLE_NAN_WITH_PAYLOAD,
            Double.NEGATIVE_INFINITY
        };
        double[] mixed = values.clone();
        for (int i = 0; i < mixed.length; i += 7) {
            mixed[i] = specials[(i / 7) % specials.length];
        }
        return mixed;
    }

    private static float[] toFloats(double[] doubles) {
        float[] floats = new float[doubles.length];
        for (int i = 0; i < doubles.length; i++) {
            floats[i] = (float) doubles[i];
        }
        return floats;
    }

    private static long[] bitsOf(double... values) {
        long[] bits = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            bits[i] = Double.doubleToRawLongBits(values[i]);
        }
        return bits;
    }

    private static int[] bitsOf(float... values) {
        int[] bits = new int[values.length];
        for (int i = 0; i < values.length; i++) {
            bits[i] = Float.floatToRawIntBits(values[i]);
        }
        return bits;
    }
}
