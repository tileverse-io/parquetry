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
package io.tileverse.parquetry.arrow.ipc;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.arrow.ipc.ArrowFieldType.ValueTransform;
import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.FixedLenBinaryVector;
import io.tileverse.parquetry.columnar.Int96Vector;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.Validity;

class ArrowValueTransformTest {

    private static ArrowFieldType noTransform() {
        return new ArrowFieldType(ArrowFieldType.Kind.INT, 32, 0, 0, 0, null, true, true, ValueTransform.NONE);
    }

    private static byte[] int96(long nanosOfDay, int julianDay) {
        ByteBuffer buffer = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(nanosOfDay);
        buffer.putInt(julianDay);
        return buffer.array();
    }

    private static ArrowFieldType int96Timestamp() {
        return new ArrowFieldType(
                ArrowFieldType.Kind.TIMESTAMP,
                64,
                0,
                0,
                0,
                ArrowFieldType.TimeUnit.MICROSECOND,
                false,
                true,
                ValueTransform.INT96_TO_TIMESTAMP);
    }

    @Test
    void int96TransformConvertsToMicrosSinceEpoch() {
        BitSet bits = new BitSet();
        bits.set(0);
        bits.set(2);
        Validity validity = Validity.of(bits, 3);
        // Row 0: epoch day, midnight -> 0 us. Row 2: epoch + 1 day, 1 ms (1_000_000 ns) -> one day + 1000 us.
        MemorySegment backing = MemorySegment.ofArray(new byte[36]);
        MemorySegment.copy(MemorySegment.ofArray(int96(0L, 2_440_588)), 0, backing, 0, 12);
        MemorySegment.copy(MemorySegment.ofArray(int96(1_000_000L, 2_440_589)), 0, backing, 24, 12);
        Int96Vector input = Int96Vector.of(backing, validity);

        ColumnVector result = ArrowValueTransform.apply(int96Timestamp(), input);

        assertThat(result).isInstanceOf(LongVector.class);
        LongVector micros = (LongVector) result;
        assertThat(micros.getLong(0)).isZero();
        assertThat(micros.getLong(2)).isEqualTo(86_400_000_000L + 1_000L);
        assertThat(micros.validity().isNull(1)).isTrue();
    }

    private static MemorySegment[] segments(byte[]... rows) {
        MemorySegment[] result = new MemorySegment[rows.length];
        for (int row = 0; row < rows.length; row++) {
            result[row] = rows[row] == null ? null : MemorySegment.ofArray(rows[row]);
        }
        return result;
    }

    private static ArrowFieldType decimal128(int precision, int scale) {
        return new ArrowFieldType(
                ArrowFieldType.Kind.DECIMAL, 128, 0, precision, scale, null, false, true, ValueTransform.DECIMAL128);
    }

    private static BigInteger unscaledLittleEndian(MemorySegment slot16) {
        byte[] le = slot16.toArray(ValueLayout.JAVA_BYTE);
        byte[] be = new byte[le.length];
        for (int i = 0; i < le.length; i++) {
            be[i] = le[le.length - 1 - i];
        }
        return new BigInteger(be);
    }

    @Test
    void decimalTransformFromInt32ProducesSixteenByteLittleEndian() {
        BitSet bits = new BitSet();
        bits.set(0);
        bits.set(2);
        Validity validity = Validity.of(bits, 3);
        IntVector input = IntVector.materialized(new int[] {12345, 0, -67890}, validity);

        FixedLenBinaryVector result = (FixedLenBinaryVector) ArrowValueTransform.apply(decimal128(9, 2), input);

        assertThat(result.byteWidth()).isEqualTo(16);
        assertThat(unscaledLittleEndian(result.get(0))).isEqualTo(BigInteger.valueOf(12345));
        assertThat(result.validity().isNull(1)).isTrue();
        assertThat(unscaledLittleEndian(result.get(2))).isEqualTo(BigInteger.valueOf(-67890));
    }

    @Test
    void decimalTransformFromBigEndianFixedLenIsSignExtendedAndReversed() {
        BitSet bits = new BitSet();
        bits.set(0, 2);
        Validity validity = Validity.of(bits, 2);
        // Big-endian two's complement: 0x00FF = 255, 0xFF00 = -256, each 2 bytes wide.
        FixedLenBinaryVector input = FixedLenBinaryVector.materialized(
                segments(new byte[] {0x00, (byte) 0xFF}, new byte[] {(byte) 0xFF, 0x00}), 2, validity);

        FixedLenBinaryVector result = (FixedLenBinaryVector) ArrowValueTransform.apply(decimal128(5, 0), input);

        assertThat(unscaledLittleEndian(result.get(0))).isEqualTo(BigInteger.valueOf(255));
        assertThat(unscaledLittleEndian(result.get(1))).isEqualTo(BigInteger.valueOf(-256));
    }

    @Test
    void decimalTransformReadsAPresentEmptyBinaryAsZero() {
        BitSet bits = new BitSet();
        bits.set(0, 2);
        Validity validity = Validity.of(bits, 2);
        // A present-but-empty BYTE_ARRAY value is a legal Parquet encoding of unscaled zero.
        BinaryVector input = BinaryVector.materialized(segments(new byte[0], new byte[] {0x2A}), validity);

        FixedLenBinaryVector result = (FixedLenBinaryVector) ArrowValueTransform.apply(decimal128(9, 0), input);

        assertThat(unscaledLittleEndian(result.get(0))).isEqualTo(BigInteger.ZERO);
        assertThat(unscaledLittleEndian(result.get(1))).isEqualTo(BigInteger.valueOf(42));
    }

    @Test
    void noneTransformReturnsTheSameVectorInstance() {
        BitSet bits = new BitSet();
        bits.set(0, 2);
        Validity validity = Validity.of(bits, 2);
        ColumnVector input = IntVector.materialized(new int[] {1, 2}, validity);

        ColumnVector result = ArrowValueTransform.apply(noTransform(), input);

        assertThat(result).isSameAs(input);
    }
}
