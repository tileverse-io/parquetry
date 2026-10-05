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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;

/**
 * The order of the signed numbers held by big-endian two's complement bytes, the layout of a decimal stored in
 * {@code BYTE_ARRAY} or {@code FIXED_LEN_BYTE_ARRAY}. Two values of unequal length compare as numbers too: the shorter
 * one counts as extended with its sign.
 */
final class SignedBytesOrder {

    private static final int NEGATIVE_FILL = 0xFF;
    private static final int POSITIVE_FILL = 0x00;

    private SignedBytesOrder() {}

    /** Negative when {@code a} holds the lesser number, positive when the greater, {@code 0} when both are equal. */
    static int compare(MemorySegment a, MemorySegment b) {
        boolean aNegative = isNegative(a);
        boolean bNegative = isNegative(b);
        if (aNegative != bNegative) {
            return aNegative ? -1 : 1;
        }
        // Two numbers of one sign and one length order as their bytes read unsigned.
        if (a.byteSize() == b.byteSize()) {
            return UnsignedLexOrder.compare(a, b);
        }
        return compareSignExtended(a, b, aNegative ? NEGATIVE_FILL : POSITIVE_FILL);
    }

    private static boolean isNegative(MemorySegment value) {
        return value.byteSize() > 0 && value.get(JAVA_BYTE, 0) < 0;
    }

    private static int compareSignExtended(MemorySegment a, MemorySegment b, int signFill) {
        long length = Math.max(a.byteSize(), b.byteSize());
        for (long index = 0; index < length; index++) {
            int difference = extendedByte(a, index, length, signFill) - extendedByte(b, index, length, signFill);
            if (difference != 0) {
                return difference;
            }
        }
        return 0;
    }

    /** The unsigned byte at {@code index} of {@code value} once extended on the left to {@code length} bytes. */
    private static int extendedByte(MemorySegment value, long index, long length, int signFill) {
        long padding = length - value.byteSize();
        if (index < padding) {
            return signFill;
        }
        return value.get(JAVA_BYTE, index - padding) & 0xFF;
    }
}
