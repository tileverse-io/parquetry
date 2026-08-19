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
package io.tileverse.parquetry.internal.write.page;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.schema.PrimitiveKind;

class PrimitiveDictionaryEncoderTest {

    private static final long LIMIT = 64;

    @ParameterizedTest(name = "{0}")
    @MethodSource("intScenarios")
    void intParity(String name, int[] values, int pageSize, long limit) throws IOException {
        DictionaryAttemptEncoder<Integer, int[]> reference = new DictionaryAttemptEncoder<>(
                new PlainInt32Encoder(),
                PrimitiveDictionaryEncoderTest::intCarrierOf,
                v -> (long) Integer.BYTES,
                limit);
        PrimitiveDictionaryEncoder tested = new PrimitiveDictionaryEncoder(PrimitiveKind.INT32, limit);
        for (int i = 0; i < values.length; i++) {
            reference.appendValue(values[i]);
            tested.appendInt(values[i]);
            if ((i + 1) % pageSize == 0 || i == values.length - 1) {
                assertPageFlushIdentical(reference, tested, name + " page ending at " + i);
            }
        }
        assertThat(tested.overflowed()).isEqualTo(reference.overflowed());
        assertThat(tested.emittedDictionaryPage()).isEqualTo(reference.emittedDictionaryPage());
        assertThat(tested.intCarrier()).isEqualTo(intCarrierOf(reference.dictionaryValues()));
    }

    static Stream<Arguments> intScenarios() {
        Random random = new Random(7);
        int[] lowCardinality = new int[300];
        for (int i = 0; i < lowCardinality.length; i++) {
            lowCardinality[i] = random.nextInt(8);
        }
        int[] overflowing = new int[300];
        for (int i = 0; i < overflowing.length; i++) {
            overflowing[i] = i; // 300 distinct ints x 4 bytes overflows a 64-byte budget quickly
        }
        int[] growth = new int[9000];
        for (int i = 0; i < growth.length; i++) {
            growth[i] = random.nextInt(3000); // ~3000 distinct forces table growth past 1024 slots
        }
        return Stream.of(
                Arguments.of("lowCardinality", lowCardinality, 64, LIMIT),
                Arguments.of("overflowMidPage", overflowing, 50, LIMIT),
                Arguments.of("overflowAfterDictPage", overflowing, 4, 40L),
                Arguments.of("tableGrowth", growth, 1000, Long.MAX_VALUE));
    }

    @Test
    void longParityWithOverflow() throws IOException {
        DictionaryAttemptEncoder<Long, long[]> reference = new DictionaryAttemptEncoder<>(
                new PlainInt64Encoder(), PrimitiveDictionaryEncoderTest::longCarrierOf, v -> (long) Long.BYTES, LIMIT);
        PrimitiveDictionaryEncoder tested = new PrimitiveDictionaryEncoder(PrimitiveKind.INT64, LIMIT);
        Random random = new Random(11);
        for (int i = 0; i < 200; i++) {
            long v = random.nextInt(40); // repeats + enough distinct to overflow 64 bytes at 8 bytes each
            reference.appendValue(v);
            tested.appendLong(v);
            if ((i + 1) % 25 == 0 || i == 199) {
                assertPageFlushIdentical(reference, tested, "long page ending at " + i);
            }
        }
        assertThat(tested.longCarrier()).isEqualTo(longCarrierOf(reference.dictionaryValues()));
        assertThat(tested.overflowed()).isEqualTo(reference.overflowed());
    }

    @Test
    void floatNaNDeduplicatesCanonicallyAndKeepsFirstRawBits() throws IOException {
        DictionaryAttemptEncoder<Float, float[]> reference = new DictionaryAttemptEncoder<>(
                new PlainFloatEncoder(),
                PrimitiveDictionaryEncoderTest::floatCarrierOf,
                v -> (long) Float.BYTES,
                LIMIT);
        PrimitiveDictionaryEncoder tested = new PrimitiveDictionaryEncoder(PrimitiveKind.FLOAT, LIMIT);
        // Non-canonical quiet NaN FIRST: equality collapses all NaNs, the dictionary must keep THESE raw bits.
        float oddNaN = Float.intBitsToFloat(0x7fc00001);
        float[] values = {oddNaN, Float.NaN, 1.5f, oddNaN, -0.0f, +0.0f, 1.5f, Float.NaN};
        for (float v : values) {
            reference.appendValue(v);
            tested.appendFloat(v);
        }
        assertPageFlushIdentical(reference, tested, "float NaN/zero page");
        float[] carrier = tested.floatCarrier();
        // Distinct entries: NaN (one entry, first raw bits), 1.5f, -0.0f, +0.0f.
        assertThat(carrier).hasSize(4);
        assertThat(Float.floatToRawIntBits(carrier[0])).isEqualTo(0x7fc00001);
        assertThat(Float.floatToRawIntBits(carrier[2])).isEqualTo(Float.floatToRawIntBits(-0.0f));
        assertThat(Float.floatToRawIntBits(carrier[3])).isEqualTo(Float.floatToRawIntBits(+0.0f));
        assertThat(carrier).isEqualTo(floatCarrierOf(reference.dictionaryValues()));
    }

    @Test
    void doubleParityWithNaNAndZeroes() throws IOException {
        DictionaryAttemptEncoder<Double, double[]> reference = new DictionaryAttemptEncoder<>(
                new PlainDoubleEncoder(),
                PrimitiveDictionaryEncoderTest::doubleCarrierOf,
                v -> (long) Double.BYTES,
                LIMIT);
        PrimitiveDictionaryEncoder tested = new PrimitiveDictionaryEncoder(PrimitiveKind.DOUBLE, LIMIT);
        double oddNaN = Double.longBitsToDouble(0x7ff8000000000001L);
        double[] values = {2.5d, oddNaN, Double.NaN, -0.0d, +0.0d, 2.5d, oddNaN};
        for (double v : values) {
            reference.appendValue(v);
            tested.appendDouble(v);
        }
        assertPageFlushIdentical(reference, tested, "double NaN/zero page");
        assertThat(tested.doubleCarrier()).isEqualTo(doubleCarrierOf(reference.dictionaryValues()));
    }

    @Test
    void clusteredFloatKeysStayFastAndByteIdentical() throws IOException {
        DictionaryAttemptEncoder<Float, float[]> reference = new DictionaryAttemptEncoder<>(
                new PlainFloatEncoder(),
                PrimitiveDictionaryEncoderTest::floatCarrierOf,
                v -> (long) Float.BYTES,
                Long.MAX_VALUE);
        PrimitiveDictionaryEncoder tested = new PrimitiveDictionaryEncoder(PrimitiveKind.FLOAT, Long.MAX_VALUE);
        // Linearly spaced coordinate-like floats: adjacent bit patterns, the exact shape that clusters an
        // identity-hashed open-addressed table into long probe runs.
        for (int i = 0; i < 150_000; i++) {
            float v = -125.0f + i * 1e-4f;
            reference.appendValue(v);
            tested.appendFloat(v);
        }
        assertPageFlushIdentical(reference, tested, "clustered float page");
        assertThat(tested.floatCarrier()).isEqualTo(floatCarrierOf(reference.dictionaryValues()));
    }

    @Test
    void rejectsNonNumericKinds() {
        assertThatThrownBy(() -> new PrimitiveDictionaryEncoder(PrimitiveKind.BYTE_ARRAY, LIMIT))
                .isInstanceOf(ParquetWriteException.class);
    }

    private static void assertPageFlushIdentical(
            PageDictionaryEncoder reference, PageDictionaryEncoder tested, String context) throws IOException {
        GrowableByteSink referenceBytes = new GrowableByteSink(64);
        PageDictionaryEncoder.PageResult referenceResult = reference.flushPage(referenceBytes);
        GrowableByteSink testedBytes = new GrowableByteSink(64);
        PageDictionaryEncoder.PageResult testedResult = tested.flushPage(testedBytes);
        assertThat(testedBytes.toByteArray()).as(context).isEqualTo(referenceBytes.toByteArray());
        assertThat(testedResult).as(context).isEqualTo(referenceResult);
    }

    private static int[] intCarrierOf(List<Integer> values) {
        int[] out = new int[values.size()];
        for (int i = 0; i < values.size(); i++) {
            out[i] = values.get(i);
        }
        return out;
    }

    private static long[] longCarrierOf(List<Long> values) {
        long[] out = new long[values.size()];
        for (int i = 0; i < values.size(); i++) {
            out[i] = values.get(i);
        }
        return out;
    }

    private static float[] floatCarrierOf(List<Float> values) {
        float[] out = new float[values.size()];
        for (int i = 0; i < values.size(); i++) {
            out[i] = values.get(i);
        }
        return out;
    }

    private static double[] doubleCarrierOf(List<Double> values) {
        double[] out = new double[values.size()];
        for (int i = 0; i < values.size(); i++) {
            out[i] = values.get(i);
        }
        return out;
    }
}
