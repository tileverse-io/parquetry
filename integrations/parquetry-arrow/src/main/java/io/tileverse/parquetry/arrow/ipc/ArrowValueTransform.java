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
package io.tileverse.parquetry.arrow.ipc;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigInteger;
import java.nio.ByteOrder;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.FixedLenBinaryVector;
import io.tileverse.parquetry.columnar.Int96Vector;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.Validity;

/**
 * Rewrites a consolidated leaf vector to the column vector whose bytes match the chosen Arrow type, for the leaf kinds
 * whose Arrow value bytes differ from the Parquet bytes the reader produced. Most leaves need no rewrite and pass
 * through unchanged. The transform runs after dictionary expansion, immediately before the Arrow buffer codec encodes
 * the vector.
 */
final class ArrowValueTransform {

    // Parquet stores INT96 little-endian; the native default would misread on big-endian hosts.
    private static final ValueLayout.OfLong NANOS_OF_DAY =
            ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfInt JULIAN_DAY =
            ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    private static final long JULIAN_DAY_OF_EPOCH = 2_440_588L;
    private static final long MICROS_PER_DAY = 86_400_000_000L;
    private static final long NANOS_PER_MICRO = 1_000L;
    private static final int INT96_WIDTH = 12;

    private ArrowValueTransform() {}

    static ColumnVector apply(ArrowFieldType type, ColumnVector consolidated) {
        return switch (type.valueTransform()) {
            case NONE -> consolidated;
            case INT96_TO_TIMESTAMP -> int96ToMicros((Int96Vector) consolidated);
            case DECIMAL128 -> toDecimal128(consolidated);
        };
    }

    private static final int DECIMAL128_WIDTH = 16;

    /**
     * Converts a consolidated decimal column to Arrow Decimal128: a 16-byte little-endian two's-complement unscaled
     * value per row. Parquet holds the unscaled integer either as INT32/INT64 or as a big-endian two's-complement byte
     * string (FIXED_LEN_BYTE_ARRAY or BYTE_ARRAY). An INT32/INT64 value writes its sign-extended little-endian bytes
     * directly; a byte-string value reads through a {@link BigInteger}. Scale and precision live in the Arrow schema
     * rather than in the bytes, leaving the unscaled value to copy across unchanged. Null rows leave a zeroed 16-byte
     * slot the validity mask masks out.
     */
    private static FixedLenBinaryVector toDecimal128(ColumnVector vector) {
        Validity validity = vector.validity();
        int size = vector.size();
        MemorySegment backing = MemorySegment.ofArray(new byte[Math.multiplyExact(size, DECIMAL128_WIDTH)]);
        for (int row = 0; row < size; row++) {
            if (validity.isNull(row)) {
                continue;
            }
            writeUnscaled(vector, row, backing, (long) row * DECIMAL128_WIDTH);
        }
        return FixedLenBinaryVector.of(backing.asReadOnly(), DECIMAL128_WIDTH, validity);
    }

    private static void writeUnscaled(ColumnVector vector, int row, MemorySegment target, long offset) {
        switch (vector) {
            case IntVector intVector -> writeLongLittleEndian(intVector.getInt(row), target, offset);
            case LongVector longVector -> writeLongLittleEndian(longVector.getLong(row), target, offset);
            case FixedLenBinaryVector fixedVector ->
                writeLittleEndian(bigEndianTwosComplement(fixedVector.get(row)), target, offset);
            case BinaryVector binaryVector ->
                writeLittleEndian(bigEndianTwosComplement(binaryVector.get(row)), target, offset);
            default ->
                throw new IllegalStateException("decimal transform does not support vector "
                        + vector.getClass().getSimpleName());
        }
    }

    /**
     * Writes a 64-bit signed {@code value} as a 16-byte little-endian two's-complement value at {@code offset},
     * sign-extended to the full width. This is the allocation-free path for INT32/INT64-backed decimals, avoiding the
     * {@link BigInteger} a byte-string value needs.
     */
    private static void writeLongLittleEndian(long value, MemorySegment target, long offset) {
        byte sign = (byte) (value < 0 ? 0xFF : 0x00);
        for (int i = 0; i < DECIMAL128_WIDTH; i++) {
            byte b = i < Long.BYTES ? (byte) (value >>> (i * Byte.SIZE)) : sign;
            target.set(ValueLayout.JAVA_BYTE, offset + i, b);
        }
    }

    /**
     * Reads a big-endian two's-complement byte string as a {@link BigInteger}. A present-but-empty value (a legal
     * Parquet encoding of unscaled zero) reads as zero rather than throwing on a zero-length BigInteger.
     */
    private static BigInteger bigEndianTwosComplement(MemorySegment value) {
        if (value.byteSize() == 0) {
            return BigInteger.ZERO;
        }
        return new BigInteger(value.toArray(ValueLayout.JAVA_BYTE));
    }

    /**
     * Writes {@code unscaled} as a 16-byte little-endian two's-complement value at {@code offset}. The value is
     * sign-extended to the full width; a value that does not fit is a schema mismatch and is rejected.
     */
    private static void writeLittleEndian(BigInteger unscaled, MemorySegment target, long offset) {
        byte[] bigEndian = unscaled.toByteArray();
        if (bigEndian.length > DECIMAL128_WIDTH) {
            throw new IllegalStateException("decimal value does not fit in 128 bits: " + bigEndian.length + " bytes");
        }
        byte sign = (byte) (unscaled.signum() < 0 ? 0xFF : 0x00);
        for (int i = 0; i < DECIMAL128_WIDTH; i++) {
            byte b = i < bigEndian.length ? bigEndian[bigEndian.length - 1 - i] : sign;
            target.set(ValueLayout.JAVA_BYTE, offset + i, b);
        }
    }

    /**
     * Converts a consolidated INT96 timestamp column to int64 microseconds-since-epoch. Each 12-byte value holds an
     * int64 nanoseconds-of-day (little-endian, bytes 0-7) and an int32 Julian day (little-endian, bytes 8-11). Null
     * rows leave a zero microsecond value the validity mask masks out. Sub-microsecond nanoseconds are truncated, the
     * precision the microsecond Arrow target cannot hold.
     */
    private static LongVector int96ToMicros(Int96Vector vector) {
        Validity validity = vector.validity();
        int size = vector.size();
        MemorySegment backing = vector.consolidatedBacking();
        long[] micros = new long[size];
        for (int row = 0; row < size; row++) {
            if (validity.isNull(row)) {
                continue;
            }
            long offset = (long) row * INT96_WIDTH;
            long nanosOfDay = backing.get(NANOS_OF_DAY, offset);
            int julianDay = backing.get(JULIAN_DAY, offset + Long.BYTES);
            micros[row] = (julianDay - JULIAN_DAY_OF_EPOCH) * MICROS_PER_DAY + nanosOfDay / NANOS_PER_MICRO;
        }
        return LongVector.materialized(micros, validity);
    }
}
