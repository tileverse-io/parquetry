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
package io.tileverse.parquetry.internal.batch;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.FixedLenBinaryVector;
import io.tileverse.parquetry.batch.Int96Vector;
import io.tileverse.parquetry.batch.Validity;
import io.tileverse.parquetry.internal.arrow.buffer.ArrowBuffers;
import io.tileverse.parquetry.internal.arrow.buffer.EncodedNode;
import io.tileverse.parquetry.internal.arrow.buffer.LeafType;

class ArrowBufferCodecBinaryTest {

    @Test
    void binaryVectorRoundTripsZeroBaseOffsets() {
        byte[] hello = utf8("hello");
        byte[] world = utf8("world");
        byte[] backingBytes = concat(hello, world);
        // Rows: "hello", null, "" (present-empty), "world". Offsets index into the backing.
        int[] offsets = {0, 5, 5, 5, 10};
        BitSet valid = new BitSet(4);
        valid.set(0);
        valid.set(2);
        valid.set(3); // row 1 null
        BinaryVector original = BinaryVector.of(heap(backingBytes), offsets, Validity.of(valid, 4));

        BinaryVector restored = (BinaryVector) roundTrip(original, LeafType.BINARY);

        assertThat(bytesAt(restored, 0)).containsExactly(hello);
        assertThat(restored.get(1)).isNull();
        assertThat(restored.get(2)).isNotNull();
        assertThat(bytesAt(restored, 2)).isEmpty();
        assertThat(bytesAt(restored, 3)).containsExactly(world);
        assertThat(restored.validity().isNull(1)).isTrue();
        assertThat(restored.validity().isValid(2)).isTrue();
    }

    @Test
    void binaryVectorRoundTripsNonZeroBaseOffsets() {
        // A 4-byte prefix the vector does not cover, then "alpha" and "beta".
        byte[] prefix = utf8("xxxx");
        byte[] alpha = utf8("alpha");
        byte[] beta = utf8("beta");
        byte[] backingBytes = concat(concat(prefix, alpha), beta);
        // The window starts at byte 4: offsets[0] == 4 (non-zero base), forcing normalization.
        int[] offsets = {4, 9, 9, 13};
        BitSet valid = new BitSet(3);
        valid.set(0);
        valid.set(2); // row 1 null
        BinaryVector original = BinaryVector.of(heap(backingBytes), offsets, Validity.of(valid, 3));

        BinaryVector restored = (BinaryVector) roundTrip(original, LeafType.BINARY);

        assertThat(bytesAt(restored, 0)).containsExactly(alpha);
        assertThat(restored.get(1)).isNull();
        assertThat(bytesAt(restored, 2)).containsExactly(beta);
    }

    @Test
    void binaryVectorRoundTripsAllNull() {
        BitSet valid = new BitSet(2); // both rows null
        int[] offsets = {0, 0, 0};
        BinaryVector original = BinaryVector.of(heap(new byte[0]), offsets, Validity.of(valid, 2));

        BinaryVector restored = (BinaryVector) roundTrip(original, LeafType.BINARY);

        assertThat(restored.size()).isEqualTo(2);
        assertThat(restored.get(0)).isNull();
        assertThat(restored.get(1)).isNull();
    }

    @Test
    void fixedLenBinaryVectorRoundTripsWithNull() {
        int byteWidth = 3;
        byte[] row0 = {1, 2, 3};
        byte[] row1 = {0, 0, 0}; // null slot, zero-filled
        byte[] row2 = {7, 8, 9};
        byte[] backingBytes = concat(concat(row0, row1), row2);
        BitSet valid = new BitSet(3);
        valid.set(0);
        valid.set(2); // row 1 null
        FixedLenBinaryVector original = FixedLenBinaryVector.of(heap(backingBytes), byteWidth, Validity.of(valid, 3));

        FixedLenBinaryVector restored = (FixedLenBinaryVector) roundTrip(original, LeafType.FIXED_LEN_BINARY);

        assertThat(restored.byteWidth()).isEqualTo(byteWidth);
        assertThat(bytesAt(restored, 0)).containsExactly(row0);
        assertThat(restored.get(1)).isNull();
        assertThat(bytesAt(restored, 2)).containsExactly(row2);
    }

    @Test
    void int96VectorRoundTripsWithNull() {
        byte[] row0 = sequentialBytes(12, 0);
        byte[] row1 = new byte[12]; // null slot, zero-filled
        byte[] row2 = sequentialBytes(12, 100);
        byte[] backingBytes = concat(concat(row0, row1), row2);
        BitSet valid = new BitSet(3);
        valid.set(0);
        valid.set(2); // row 1 null
        Int96Vector original = Int96Vector.of(heap(backingBytes), Validity.of(valid, 3));

        Int96Vector restored = (Int96Vector) roundTrip(original, LeafType.INT96);

        assertThat(bytesAt(restored, 0)).containsExactly(row0);
        assertThat(restored.get(1)).isNull();
        assertThat(bytesAt(restored, 2)).containsExactly(row2);
    }

    @Test
    void fixedLenBinaryDataBufferIsPaddedToEightBytes() {
        int byteWidth = 3;
        byte[] backingBytes = concat(concat(new byte[] {1, 2, 3}, new byte[] {4, 5, 6}), new byte[] {7, 8, 9});
        FixedLenBinaryVector original = FixedLenBinaryVector.of(heap(backingBytes), byteWidth, Validity.allValid(3));

        EncodedNode node = ArrowBufferCodec.encode(original);

        // size 3 * width 3 == 9 logical bytes; Arrow pads up to align(9) == 16.
        assertThat(lastBufferByteSize(node)).isEqualTo(ArrowBuffers.align(9)).isEqualTo(16);
    }

    @Test
    void binaryDataBufferIsPaddedToEightBytes() {
        byte[] hello = utf8("hello");
        byte[] world = utf8("world");
        byte[] backingBytes = concat(hello, world);
        int[] offsets = {0, 5, 10};
        BinaryVector original = BinaryVector.of(heap(backingBytes), offsets, Validity.allValid(2));

        EncodedNode node = ArrowBufferCodec.encode(original);

        // window end - base == 10 logical bytes; Arrow pads up to align(10) == 16.
        assertThat(lastBufferByteSize(node)).isEqualTo(ArrowBuffers.align(10)).isEqualTo(16);
    }

    private static long lastBufferByteSize(EncodedNode node) {
        int last = node.buffers().size() - 1;
        return node.buffers().get(last).bytes().byteSize();
    }

    @Test
    void offsetsRoundTripThroughArrowBuffers() {
        int[] lengths = {5, 0, 3, 7};

        MemorySegment encoded = ArrowBuffers.encodeOffsets(lengths);
        int[] decoded = ArrowBuffers.decodeOffsets(encoded, lengths.length);

        assertThat(decoded).containsExactly(0, 5, 5, 8, 15);
    }

    private static byte[] bytesAt(ColumnVector vector, int row) {
        MemorySegment value = vector.get(row);
        return value.toArray(JAVA_BYTE);
    }

    private static MemorySegment heap(byte[] bytes) {
        return MemorySegment.ofArray(bytes);
    }

    private static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] concat(byte[] first, byte[] second) {
        byte[] out = new byte[first.length + second.length];
        System.arraycopy(first, 0, out, 0, first.length);
        System.arraycopy(second, 0, out, first.length, second.length);
        return out;
    }

    private static byte[] sequentialBytes(int length, int start) {
        byte[] out = new byte[length];
        for (int i = 0; i < length; i++) {
            out[i] = (byte) (start + i);
        }
        return out;
    }

    private static ColumnVector roundTrip(ColumnVector vector, LeafType type) {
        EncodedNode node = ArrowBufferCodec.encode(vector);
        return ArrowBufferCodec.decode(node, type);
    }
}
