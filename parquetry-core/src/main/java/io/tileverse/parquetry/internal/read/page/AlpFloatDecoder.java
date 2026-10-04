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

import java.lang.foreign.MemorySegment;

import io.tileverse.parquetry.format.ParquetLayouts;

/**
 * ALP (Adaptive Lossless floating-Point) page decoder for FLOAT values. A value decodes from its encoded integer as
 * {@code (float) encoded * 10^factor * 10^-exponent} in binary32 arithmetic, the decoding formula of parquet-format's
 * AlpEncoding.md; exceptions replace their values with the stored bits. Values decode straight into the caller's
 * buffers.
 *
 * @see AlpPageCursor for the page layout and its validation
 */
public final class AlpFloatDecoder implements PageDecoder<Float> {

    /**
     * Correctly rounded binary32 values of 1e0 to 1e10, written as literals because the specification forbids pow().
     */
    private static final float[] POWERS_OF_TEN = {1e0f, 1e1f, 1e2f, 1e3f, 1e4f, 1e5f, 1e6f, 1e7f, 1e8f, 1e9f, 1e10f};

    /** Correctly rounded binary32 values of 1e-0 to 1e-10. */
    private static final float[] NEGATIVE_POWERS_OF_TEN = {
        1e-0f, 1e-1f, 1e-2f, 1e-3f, 1e-4f, 1e-5f, 1e-6f, 1e-7f, 1e-8f, 1e-9f, 1e-10f
    };

    private final AlpPageCursor cursor = new AlpPageCursor(AlpPageCursor.ValueType.FLOAT);
    private final float[] single = new float[1];

    @Override
    public void load(MemorySegment page, int valueCount) {
        cursor.load(page);
    }

    @Override
    public Float next() {
        decodeFloats(1, single, 0);
        return single[0];
    }

    @Override
    public void skip(int n) {
        cursor.skip(n);
    }

    @Override
    public void decodeFloats(int n, float[] dst, int offset) {
        cursor.requireAvailable(n);
        int decoded = 0;
        while (decoded < n) {
            int run = cursor.beginRun(n - decoded);
            decodeRun(run, dst, offset + decoded);
            cursor.endRun(run);
            decoded += run;
        }
    }

    @Override
    public void decodeFloats(int n, MemorySegment dst, long dstIndex) {
        cursor.requireAvailable(n);
        int decoded = 0;
        while (decoded < n) {
            int run = cursor.beginRun(n - decoded);
            decodeRun(run, dst, dstIndex + decoded);
            cursor.endRun(run);
            decoded += run;
        }
    }

    private void decodeRun(int run, float[] dst, int target) {
        int first = cursor.runStart();
        int frame = (int) cursor.frameOfReference();
        float factorPower = POWERS_OF_TEN[cursor.factor()];
        float exponentPower = NEGATIVE_POWERS_OF_TEN[cursor.exponent()];
        for (int i = 0; i < run; i++) {
            int encoded = frame + (int) cursor.packedDelta(first + i);
            dst[target + i] = decode(encoded, factorPower, exponentPower);
        }
        int exception = cursor.firstExceptionInRun();
        while (exception >= 0) {
            int slot = target + cursor.exceptionPosition(exception) - first;
            dst[slot] = Float.intBitsToFloat(cursor.exceptionIntBits(exception));
            exception = cursor.nextExceptionInRun(exception);
        }
    }

    /** Writes exceptions as raw bits to keep NaN payloads intact. */
    private void decodeRun(int run, MemorySegment dst, long target) {
        int first = cursor.runStart();
        int frame = (int) cursor.frameOfReference();
        float factorPower = POWERS_OF_TEN[cursor.factor()];
        float exponentPower = NEGATIVE_POWERS_OF_TEN[cursor.exponent()];
        for (int i = 0; i < run; i++) {
            int encoded = frame + (int) cursor.packedDelta(first + i);
            dst.setAtIndex(ParquetLayouts.FLOAT, target + i, decode(encoded, factorPower, exponentPower));
        }
        int exception = cursor.firstExceptionInRun();
        while (exception >= 0) {
            long slot = target + cursor.exceptionPosition(exception) - first;
            dst.setAtIndex(ParquetLayouts.INT32, slot, cursor.exceptionIntBits(exception));
            exception = cursor.nextExceptionInRun(exception);
        }
    }

    /**
     * The specification's decoding formula. The int-to-float conversion and both multiplications round in binary32,
     * left to right: folding the two powers into one constant would change the rounding and break lossless reads.
     */
    private static float decode(int encoded, float factorPower, float exponentPower) {
        return (float) encoded * factorPower * exponentPower;
    }
}
