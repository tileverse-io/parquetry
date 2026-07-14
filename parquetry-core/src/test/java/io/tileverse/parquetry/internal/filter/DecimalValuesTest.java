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
package io.tileverse.parquetry.internal.filter;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.math.BigInteger;

import org.junit.jupiter.api.Test;

class DecimalValuesTest {

    private static MemorySegment flba(int... unsignedBytes) {
        byte[] bytes = new byte[unsignedBytes.length];
        for (int i = 0; i < unsignedBytes.length; i++) {
            bytes[i] = (byte) unsignedBytes[i];
        }
        return MemorySegment.ofArray(bytes);
    }

    @Test
    void signedLongDecodesPositiveBigEndian() {
        assertThat(DecimalValues.signedLong(flba(0x00, 0x00, 0x01, 0x2C))).isEqualTo(300L);
    }

    @Test
    void signedLongDecodesNegativeTwosComplement() {
        assertThat(DecimalValues.signedLong(flba(0xFF, 0xFF, 0xFE, 0xD4))).isEqualTo(-300L);
    }

    @Test
    void toBigDecimalAppliesScale() {
        assertThat(DecimalValues.toBigDecimal(flba(0xFF, 0xFF, 0xFE, 0xD4), 2)).isEqualByComparingTo("-3.00");
    }

    @Test
    void signedBigDecodesWideNegative() {
        BigInteger wide = new BigInteger("-1234567890123456789012345");
        MemorySegment seg = MemorySegment.ofArray(wide.toByteArray());
        assertThat(seg.byteSize()).isGreaterThan(8);
        assertThat(DecimalValues.signedBig(seg)).isEqualTo(wide);
    }

    @Test
    void signedBigDecodesWidePositive() {
        BigInteger wide = new BigInteger("1234567890123456789012345");
        MemorySegment seg = MemorySegment.ofArray(wide.toByteArray());
        assertThat(seg.byteSize()).isGreaterThan(8);
        assertThat(DecimalValues.signedBig(seg)).isEqualTo(wide);
    }
}
