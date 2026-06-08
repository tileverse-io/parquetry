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
package io.tileverse.parquetry.arrow;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.BooleanVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DoubleVector;
import io.tileverse.parquetry.batch.FixedLenBinaryVector;
import io.tileverse.parquetry.batch.FloatVector;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.LongVector;
import io.tileverse.parquetry.batch.Validity;
import io.tileverse.parquetry.format.UnsupportedFeatureException;

/** Lays out the Arrow body buffers for one {@link ColumnVector}, each padded to an 8-byte boundary. */
final class ColumnBuffers {

    private static final int ALIGNMENT = 8;

    private ColumnBuffers() {}

    static List<byte[]> forVector(ColumnVector vector) {
        List<byte[]> buffers = new ArrayList<>();
        buffers.add(validityBuffer(vector.validity(), vector.size()));
        switch (vector) {
            case BooleanVector v -> buffers.add(booleanData(v));
            case IntVector v -> buffers.add(fixedWidth(v));
            case LongVector v -> buffers.add(fixedWidth(v));
            case FloatVector v -> buffers.add(floatData(v));
            case DoubleVector v -> buffers.add(doubleData(v));
            case BinaryVector v -> addVariableWidth(buffers, v);
            case FixedLenBinaryVector v -> addFixedSizeBinary(buffers, v);
            default ->
                throw new UnsupportedFeatureException("Arrow output does not support column vector "
                        + vector.getClass().getSimpleName());
        }
        return buffers;
    }

    private static byte[] validityBuffer(Validity validity, int size) {
        int byteLength = (size + 7) / 8;
        BitSet validBits = validity.copy();
        // BitSet.toByteArray() is LSB-first within each byte, which is exactly the Arrow validity bitmap format.
        byte[] raw = validBits.toByteArray();
        byte[] padded = new byte[align(Math.max(byteLength, 1))];
        System.arraycopy(raw, 0, padded, 0, Math.min(raw.length, padded.length));
        return padded;
    }

    private static byte[] booleanData(BooleanVector vector) {
        // Arrow stores a value for every slot, null rows included; the separate validity buffer marks nulls.
        int size = vector.size();
        byte[] out = new byte[align(Math.max((size + 7) / 8, 1))];
        vector.copyInto(MemorySegment.ofArray(out), 0L, 0, size);
        return out;
    }

    private static byte[] fixedWidth(IntVector vector) {
        int size = vector.size();
        byte[] out = new byte[align(size * Integer.BYTES)];
        vector.copyInto(MemorySegment.ofArray(out), 0L, 0, size);
        return out;
    }

    private static byte[] fixedWidth(LongVector vector) {
        int size = vector.size();
        byte[] out = new byte[align(size * Long.BYTES)];
        vector.copyInto(MemorySegment.ofArray(out), 0L, 0, size);
        return out;
    }

    private static byte[] floatData(FloatVector vector) {
        int size = vector.size();
        byte[] out = new byte[align(size * Float.BYTES)];
        vector.copyInto(MemorySegment.ofArray(out), 0L, 0, size);
        return out;
    }

    private static byte[] doubleData(DoubleVector vector) {
        int size = vector.size();
        byte[] out = new byte[align(size * Double.BYTES)];
        vector.copyInto(MemorySegment.ofArray(out), 0L, 0, size);
        return out;
    }

    private static void addVariableWidth(List<byte[]> buffers, BinaryVector vector) {
        int size = vector.size();
        // Arrow variable-width layout: int32 offsets (size+1 entries) + concatenated value bytes.
        ByteBuffer offsets =
                ByteBuffer.allocate(align((size + 1) * Integer.BYTES)).order(ByteOrder.LITTLE_ENDIAN);
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        int running = 0;
        offsets.putInt(0);
        for (int row = 0; row < size; row++) {
            if (vector.isValid(row)) {
                byte[] bytes = segmentToBytes(vector.get(row));
                data.writeBytes(bytes);
                running += bytes.length;
            }
            offsets.putInt(running);
        }
        buffers.add(offsets.array());
        buffers.add(pad(data.toByteArray()));
    }

    private static void addFixedSizeBinary(List<byte[]> buffers, FixedLenBinaryVector vector) {
        int width = vector.byteWidth();
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        for (int row = 0; row < vector.size(); row++) {
            if (vector.isValid(row)) {
                data.writeBytes(segmentToBytes(vector.get(row)));
            } else {
                // Null slots are written as zero bytes to keep fixed stride intact.
                data.writeBytes(new byte[width]);
            }
        }
        buffers.add(pad(data.toByteArray()));
    }

    private static byte[] segmentToBytes(MemorySegment segment) {
        return segment.toArray(ValueLayout.JAVA_BYTE);
    }

    private static byte[] pad(byte[] bytes) {
        int paddedLength = align(bytes.length);
        if (paddedLength == bytes.length) {
            return bytes;
        }
        byte[] result = new byte[paddedLength];
        System.arraycopy(bytes, 0, result, 0, bytes.length);
        return result;
    }

    private static int align(int value) {
        int remainder = value % ALIGNMENT;
        return remainder == 0 ? value : value + (ALIGNMENT - remainder);
    }
}
