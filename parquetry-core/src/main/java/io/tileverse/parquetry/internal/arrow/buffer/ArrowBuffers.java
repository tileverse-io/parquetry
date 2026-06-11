/*
 * Copyright (c) 2026 Multivers.io
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
package io.tileverse.parquetry.internal.arrow.buffer;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.BitSet;

import io.tileverse.parquetry.batch.BooleanVector;
import io.tileverse.parquetry.batch.DoubleVector;
import io.tileverse.parquetry.batch.FloatVector;
import io.tileverse.parquetry.batch.IntSequence;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.LongVector;
import io.tileverse.parquetry.batch.Validity;

/**
 * Builds and reads the fixed-layout byte buffers of an Arrow field node: the validity bitmap and the little-endian
 * fixed-width value/offsets arrays.
 *
 * <p>Every produced buffer is padded up to an 8-byte boundary, which keeps consecutive buffers aligned within a packed
 * payload as Arrow's IPC layout expects. Buffers are returned read-only.
 *
 * <p>The validity bitmap, offsets, and fixed-width value layouts and the buffer alignment rule follow the Apache Arrow
 * columnar format specification.
 *
 * @see <a href="https://arrow.apache.org/docs/format/Columnar.html">Apache Arrow columnar format specification</a>
 */
public final class ArrowBuffers {

    private static final int ALIGNMENT = 8;
    private static final int INT_BYTES = Integer.BYTES;
    private static final int LONG_BYTES = Long.BYTES;
    private static final int FLOAT_BYTES = Float.BYTES;
    private static final int DOUBLE_BYTES = Double.BYTES;
    private static final int BITS_PER_BYTE = 8;

    private static final ValueLayout.OfInt INT32 = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfLong INT64 = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfFloat FLOAT =
            ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfDouble DOUBLE =
            ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    private ArrowBuffers() {
        // utility
    }

    /** Rounds {@code byteLength} up to the next multiple of the 8-byte Arrow buffer alignment. */
    public static int align(int byteLength) {
        return (byteLength + ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT;
    }

    /**
     * Copies {@code source} into a freshly allocated read-only heap segment padded up to the 8-byte Arrow buffer
     * alignment. The trailing pad bytes are zero; readers bound their reads by the field node length and ignore them.
     */
    public static MemorySegment paddedCopy(MemorySegment source) {
        int length = (int) source.byteSize();
        byte[] padded = new byte[align(length)];
        MemorySegment.copy(source, ValueLayout.JAVA_BYTE, 0, padded, 0, length);
        return MemorySegment.ofArray(padded).asReadOnly();
    }

    /**
     * Encodes a validity mask as an Arrow validity bitmap (LSB-first, a set bit meaning the row is valid). An all-valid
     * mask needs no bitmap and encodes to a zero-length buffer.
     */
    public static MemorySegment encodeValidity(Validity validity) {
        if (!validity.hasNulls()) {
            return MemorySegment.ofArray(new byte[0]).asReadOnly();
        }
        byte[] lsbFirst = validity.copy().toByteArray();
        int minBytes = Math.max((validity.size() + 7) / 8, 1);
        byte[] padded = new byte[align(minBytes)];
        System.arraycopy(lsbFirst, 0, padded, 0, lsbFirst.length);
        return MemorySegment.ofArray(padded).asReadOnly();
    }

    /**
     * Decodes an Arrow validity bitmap back into a {@link Validity} mask. A zero null count or empty buffer means the
     * column was written all-valid.
     */
    public static Validity decodeValidity(MemorySegment validityBuffer, int nullCount, int size) {
        if (nullCount == 0 || validityBuffer.byteSize() == 0) {
            return Validity.allValid(size);
        }
        byte[] bytes = validityBuffer.toArray(ValueLayout.JAVA_BYTE);
        return Validity.of(BitSet.valueOf(bytes), size);
    }

    /** Packs {@code values} as a little-endian {@code int32} array, padded to the 8-byte alignment. */
    public static MemorySegment encodeInts(int[] values) {
        MemorySegment segment = MemorySegment.ofArray(new byte[align(values.length * INT_BYTES)]);
        for (int i = 0; i < values.length; i++) {
            segment.setAtIndex(INT32, i, values[i]);
        }
        return segment.asReadOnly();
    }

    /** Packs a sequence's {@code int32} values as a little-endian array, padded to the 8-byte alignment. */
    public static MemorySegment encodeInts(IntSequence values) {
        MemorySegment segment = MemorySegment.ofArray(new byte[align(values.size() * INT_BYTES)]);
        values.copyInto(segment, 0L);
        return segment.asReadOnly();
    }

    /** Packs a vector's {@code int32} values as a little-endian array, padded to the 8-byte alignment. */
    public static MemorySegment encodeInts(IntVector vector) {
        int size = vector.size();
        MemorySegment segment = MemorySegment.ofArray(new byte[align(size * INT_BYTES)]);
        vector.copyInto(segment, 0L, 0, size);
        return segment.asReadOnly();
    }

    /** Reads the first {@code count} little-endian {@code int32} values from {@code buffer}. */
    public static int[] decodeInts(MemorySegment buffer, int count) {
        int[] values = new int[count];
        for (int i = 0; i < count; i++) {
            values[i] = buffer.getAtIndex(INT32, i);
        }
        return values;
    }

    /** Packs a vector's {@code int64} values as a little-endian array, padded to the 8-byte alignment. */
    public static MemorySegment encodeLongs(LongVector vector) {
        int size = vector.size();
        MemorySegment segment = MemorySegment.ofArray(new byte[align(size * LONG_BYTES)]);
        vector.copyInto(segment, 0L, 0, size);
        return segment.asReadOnly();
    }

    /** Reads the first {@code count} little-endian {@code int64} values from {@code buffer}. */
    public static long[] decodeLongs(MemorySegment buffer, int count) {
        long[] values = new long[count];
        for (int i = 0; i < count; i++) {
            values[i] = buffer.getAtIndex(INT64, i);
        }
        return values;
    }

    /** Packs a vector's IEEE-754 {@code float} values as a little-endian array, padded to the 8-byte alignment. */
    public static MemorySegment encodeFloats(FloatVector vector) {
        int size = vector.size();
        MemorySegment segment = MemorySegment.ofArray(new byte[align(size * FLOAT_BYTES)]);
        vector.copyInto(segment, 0L, 0, size);
        return segment.asReadOnly();
    }

    /** Reads the first {@code count} little-endian IEEE-754 {@code float} values from {@code buffer}. */
    public static float[] decodeFloats(MemorySegment buffer, int count) {
        float[] values = new float[count];
        for (int i = 0; i < count; i++) {
            values[i] = buffer.getAtIndex(FLOAT, i);
        }
        return values;
    }

    /** Packs a vector's IEEE-754 {@code double} values as a little-endian array, padded to the 8-byte alignment. */
    public static MemorySegment encodeDoubles(DoubleVector vector) {
        int size = vector.size();
        MemorySegment segment = MemorySegment.ofArray(new byte[align(size * DOUBLE_BYTES)]);
        vector.copyInto(segment, 0L, 0, size);
        return segment.asReadOnly();
    }

    /** Reads the first {@code count} little-endian IEEE-754 {@code double} values from {@code buffer}. */
    public static double[] decodeDoubles(MemorySegment buffer, int count) {
        double[] values = new double[count];
        for (int i = 0; i < count; i++) {
            values[i] = buffer.getAtIndex(DOUBLE, i);
        }
        return values;
    }

    /**
     * Packs a vector's booleans into an LSB-first bit-packed buffer (bit {@code i} set meaning row {@code i} is true),
     * padded to the 8-byte alignment, mirroring the validity bitmap layout Arrow uses for a boolean data buffer.
     */
    public static MemorySegment encodeBooleanBitmap(BooleanVector vector) {
        int size = vector.size();
        int minBytes = Math.max((size + BITS_PER_BYTE - 1) / BITS_PER_BYTE, 1);
        MemorySegment segment = MemorySegment.ofArray(new byte[align(minBytes)]);
        vector.copyInto(segment, 0L, 0, size);
        return segment.asReadOnly();
    }

    /** Reads {@code size} booleans from an LSB-first bit-packed buffer produced by {@link #encodeBooleanBitmap}. */
    public static boolean[] decodeBooleanBitmap(MemorySegment buffer, int size) {
        byte[] bytes = buffer.toArray(ValueLayout.JAVA_BYTE);
        BitSet bits = BitSet.valueOf(bytes);
        boolean[] values = new boolean[size];
        for (int i = 0; i < size; i++) {
            values[i] = bits.get(i);
        }
        return values;
    }

    /**
     * Builds an Arrow {@code int32} offsets buffer of {@code count + 1} entries from per-row element lengths. Offset
     * {@code i} is the start of row {@code i}; the final entry is the total element count.
     */
    public static MemorySegment encodeOffsets(int[] lengths) {
        int entries = lengths.length + 1;
        MemorySegment segment = MemorySegment.ofArray(new byte[align(entries * INT_BYTES)]);
        int running = 0;
        segment.setAtIndex(INT32, 0, running);
        for (int i = 0; i < lengths.length; i++) {
            running += lengths[i];
            segment.setAtIndex(INT32, i + 1L, running);
        }
        return segment.asReadOnly();
    }

    /** Reads the {@code count + 1} little-endian {@code int32} entries of an offsets buffer. */
    public static int[] decodeOffsets(MemorySegment buffer, int count) {
        return decodeInts(buffer, count + 1);
    }
}
