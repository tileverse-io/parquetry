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
package io.tileverse.parquetry.internal.write;

import static java.lang.foreign.ValueLayout.JAVA_SHORT_UNALIGNED;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * The IEEE 754 half floats of a FLOAT16 column, each stored as two little-endian bytes, and the {@link TotalOrder} keys
 * ordering them.
 */
final class HalfFloats {

    /** Byte width of a FLOAT16 cell. */
    static final int BYTES = 2;

    private static final ValueLayout.OfShort CELL = JAVA_SHORT_UNALIGNED.withOrder(LITTLE_ENDIAN);
    private static final int MAGNITUDE_MASK = 0x7FFF;
    private static final int INFINITY_MAGNITUDE = 0x7C00;

    private HalfFloats() {}

    /** The {@link TotalOrder} key of the half float held by {@code cell}. */
    static int key(MemorySegment cell) {
        return key(cell.get(CELL, 0));
    }

    /**
     * The {@link TotalOrder} key of the half float with the given bits: the key of the bits extended with their sign.
     */
    static int key(short bits) {
        int signExtended = bits;
        return TotalOrder.key(signExtended);
    }

    /** The bits of the half float with the given {@link #key(short) order key}. */
    static short bits(int key) {
        return (short) TotalOrder.bits(key);
    }

    /** Whether the half float held by {@code cell} is a NaN. */
    static boolean isNaN(MemorySegment cell) {
        return (cell.get(CELL, 0) & MAGNITUDE_MASK) > INFINITY_MAGNITUDE;
    }

    /** The two little-endian bytes of the half float with the given bits. */
    static byte[] encode(short bits) {
        byte[] cell = new byte[BYTES];
        MemorySegment.ofArray(cell).set(CELL, 0, bits);
        return cell;
    }
}
