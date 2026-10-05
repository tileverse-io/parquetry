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

/**
 * Integer keys putting the bit patterns of IEEE 754 floating-point values in total order: the negative NaNs, the
 * negative numbers from the largest magnitude down to {@code -0.0}, {@code +0.0} and the positive numbers, the positive
 * NaNs. The NaNs of one sign order by their payload bits.
 *
 * <p>Signed comparison of two keys orders their bit patterns. One transformation maps a bit pattern to its key and a
 * key back to its bit pattern.
 */
final class TotalOrder {

    private TotalOrder() {}

    /** The order key of the 32-bit pattern {@code bits}. */
    static int key(int bits) {
        return bits ^ magnitudeFlip(bits);
    }

    /** The 32-bit pattern with the order key {@code key}. */
    static int bits(int key) {
        return key ^ magnitudeFlip(key);
    }

    /** The order key of the 64-bit pattern {@code bits}. */
    static long key(long bits) {
        return bits ^ magnitudeFlip(bits);
    }

    /** The 64-bit pattern with the order key {@code key}. */
    static long bits(long key) {
        return key ^ magnitudeFlip(key);
    }

    /**
     * A mask flipping the magnitude bits of a negative pattern and leaving a positive one alone. Negative floats grow
     * in magnitude as they shrink in value, the reverse of two's complement integers.
     */
    private static int magnitudeFlip(int bits) {
        return (bits >> (Integer.SIZE - 1)) >>> 1;
    }

    private static long magnitudeFlip(long bits) {
        return (bits >> (Long.SIZE - 1)) >>> 1;
    }
}
