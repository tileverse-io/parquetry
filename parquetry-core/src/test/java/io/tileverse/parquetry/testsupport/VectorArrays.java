/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.testsupport;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import io.tileverse.parquetry.columnar.BooleanVector;
import io.tileverse.parquetry.columnar.DoubleVector;
import io.tileverse.parquetry.columnar.FloatVector;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.format.ParquetLayouts;

/**
 * Validity-blind bulk reads of the primitive vectors for tests that assert on every stored slot, null rows included.
 * Each method copies the vector's raw values through {@code copyInto} and decodes them with the little-endian Arrow
 * layouts that {@code copyInto} writes, preserving the parked value at null rows.
 */
public final class VectorArrays {

    private VectorArrays() {
        // test helper
    }

    public static int[] toArray(IntVector vector) {
        int size = vector.size();
        MemorySegment scratch = MemorySegment.ofArray(new byte[size * Integer.BYTES]);
        vector.copyInto(scratch, 0L, 0, size);
        int[] out = new int[size];
        for (int row = 0; row < size; row++) {
            out[row] = scratch.getAtIndex(ParquetLayouts.INT32, row);
        }
        return out;
    }

    public static long[] toArray(LongVector vector) {
        int size = vector.size();
        MemorySegment scratch = MemorySegment.ofArray(new byte[size * Long.BYTES]);
        vector.copyInto(scratch, 0L, 0, size);
        long[] out = new long[size];
        for (int row = 0; row < size; row++) {
            out[row] = scratch.getAtIndex(ParquetLayouts.INT64, row);
        }
        return out;
    }

    public static float[] toArray(FloatVector vector) {
        int size = vector.size();
        MemorySegment scratch = MemorySegment.ofArray(new byte[size * Float.BYTES]);
        vector.copyInto(scratch, 0L, 0, size);
        float[] out = new float[size];
        for (int row = 0; row < size; row++) {
            out[row] = scratch.getAtIndex(ParquetLayouts.FLOAT, row);
        }
        return out;
    }

    public static double[] toArray(DoubleVector vector) {
        int size = vector.size();
        MemorySegment scratch = MemorySegment.ofArray(new byte[size * Double.BYTES]);
        vector.copyInto(scratch, 0L, 0, size);
        double[] out = new double[size];
        for (int row = 0; row < size; row++) {
            out[row] = scratch.getAtIndex(ParquetLayouts.DOUBLE, row);
        }
        return out;
    }

    public static boolean[] toArray(BooleanVector vector) {
        int size = vector.size();
        MemorySegment scratch = MemorySegment.ofArray(new byte[(size + 7) / 8]);
        vector.copyInto(scratch, 0L, 0, size);
        boolean[] out = new boolean[size];
        for (int row = 0; row < size; row++) {
            int bits = scratch.get(ValueLayout.JAVA_BYTE, row >>> 3) & 0xFF;
            out[row] = ((bits >>> (row & 7)) & 1) != 0;
        }
        return out;
    }
}
