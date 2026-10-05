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
 * ALP (Adaptive Lossless floating-Point) page decoder for DOUBLE values. A value decodes from its encoded integer as
 * {@code (double) encoded * 10^factor * 10^-exponent} in binary64 arithmetic, the decoding formula of parquet-format's
 * AlpEncoding.md; exceptions replace their values with the stored bits. Values decode straight into the caller's
 * buffers.
 *
 * @see AlpPageCursor for the page layout and its validation
 */
public final class AlpDoubleDecoder implements PageDecoder<Double> {

    /**
     * Correctly rounded binary64 values of 1e0 to 1e18, written as literals because the specification forbids pow().
     */
    private static final double[] POWERS_OF_TEN = {
        1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18
    };

    /** Correctly rounded binary64 values of 1e-0 to 1e-18. */
    private static final double[] NEGATIVE_POWERS_OF_TEN = {
        1e-0, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6, 1e-7, 1e-8, 1e-9, 1e-10, 1e-11, 1e-12, 1e-13, 1e-14, 1e-15, 1e-16,
        1e-17, 1e-18
    };

    private final AlpPageCursor cursor = new AlpPageCursor(AlpPageCursor.ValueType.DOUBLE);
    private final double[] single = new double[1];

    @Override
    public void load(MemorySegment page, int valueCount) {
        cursor.load(page);
    }

    @Override
    public Double next() {
        decodeDoubles(1, single, 0);
        return single[0];
    }

    @Override
    public void skip(int n) {
        cursor.skip(n);
    }

    @Override
    public void decodeDoubles(int n, double[] dst, int offset) {
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
    public void decodeDoubles(int n, MemorySegment dst, long dstIndex) {
        cursor.requireAvailable(n);
        int decoded = 0;
        while (decoded < n) {
            int run = cursor.beginRun(n - decoded);
            decodeRun(run, dst, dstIndex + decoded);
            cursor.endRun(run);
            decoded += run;
        }
    }

    private void decodeRun(int run, double[] dst, int target) {
        int first = cursor.runStart();
        long frame = cursor.frameOfReference();
        double factorPower = POWERS_OF_TEN[cursor.factor()];
        double exponentPower = NEGATIVE_POWERS_OF_TEN[cursor.exponent()];
        for (int i = 0; i < run; i++) {
            long encoded = frame + cursor.packedDelta(first + i);
            dst[target + i] = decode(encoded, factorPower, exponentPower);
        }
        int exception = cursor.firstExceptionInRun();
        while (exception >= 0) {
            int slot = target + cursor.exceptionPosition(exception) - first;
            dst[slot] = Double.longBitsToDouble(cursor.exceptionLongBits(exception));
            exception = cursor.nextExceptionInRun(exception);
        }
    }

    /** Writes exceptions as raw bits to keep NaN payloads intact. */
    private void decodeRun(int run, MemorySegment dst, long target) {
        int first = cursor.runStart();
        long frame = cursor.frameOfReference();
        double factorPower = POWERS_OF_TEN[cursor.factor()];
        double exponentPower = NEGATIVE_POWERS_OF_TEN[cursor.exponent()];
        for (int i = 0; i < run; i++) {
            long encoded = frame + cursor.packedDelta(first + i);
            dst.setAtIndex(ParquetLayouts.DOUBLE, target + i, decode(encoded, factorPower, exponentPower));
        }
        int exception = cursor.firstExceptionInRun();
        while (exception >= 0) {
            long slot = target + cursor.exceptionPosition(exception) - first;
            dst.setAtIndex(ParquetLayouts.INT64, slot, cursor.exceptionLongBits(exception));
            exception = cursor.nextExceptionInRun(exception);
        }
    }

    /**
     * The specification's decoding formula. The long-to-double conversion and both multiplications round in binary64,
     * left to right: folding the two powers into one constant would change the rounding and break lossless reads.
     */
    private static double decode(long encoded, double factorPower, double exponentPower) {
        return (double) encoded * factorPower * exponentPower;
    }
}
