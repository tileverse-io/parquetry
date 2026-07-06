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
package io.tileverse.parquetry.variant;

import java.math.BigInteger;

/** Wire-format byte helpers shared between the Variant encoder and the shredded scalar writer. */
public final class VariantBytes {

    private VariantBytes() {}

    /** Combines a basic type with its value header into a Variant header byte. */
    public static int header(int basicType, int valueHeader) {
        return (basicType & 0x03) | (valueHeader << 2);
    }

    /**
     * Encodes {@code value} as a fixed-width little-endian two's-complement byte array, sign-extended to {@code width}.
     */
    public static byte[] twosComplementLittleEndian(BigInteger value, int width) {
        byte[] bigEndian = value.toByteArray();
        byte[] littleEndian = new byte[width];
        byte signExtension = (value.signum() < 0) ? (byte) 0xFF : (byte) 0x00;
        for (int i = 0; i < width; i++) {
            int bigEndianIndex = bigEndian.length - 1 - i;
            littleEndian[i] = (bigEndianIndex >= 0) ? bigEndian[bigEndianIndex] : signExtension;
        }
        return littleEndian;
    }
}
