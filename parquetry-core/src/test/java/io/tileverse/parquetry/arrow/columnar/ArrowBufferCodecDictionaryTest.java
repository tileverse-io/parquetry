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
package io.tileverse.parquetry.arrow.columnar;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.arrow.columnar.EncodedBuffer.BufferRole;
import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.FixedLenBinaryVector;
import io.tileverse.parquetry.columnar.Int96Vector;
import io.tileverse.parquetry.columnar.IntSequence;
import io.tileverse.parquetry.columnar.Validity;

class ArrowBufferCodecDictionaryTest {

    @Test
    void dictionaryBinaryVectorRoundTripsPreservingEncoding() {
        MemorySegment red = readOnly(utf8("red"));
        MemorySegment green = readOnly(utf8("green"));
        MemorySegment[] dictEntries = {red, green};
        // Rows: red, green, null, red, green
        int[] indices = {0, 1, 0, 0, 1};
        BitSet valid = new BitSet(5);
        valid.set(0);
        valid.set(1);
        valid.set(3);
        valid.set(4); // row 2 null
        BinaryVector original = BinaryVector.dictionary(dictEntries, IntSequence.of(indices), Validity.of(valid, 5));

        BinaryVector restored = (BinaryVector) roundTrip(original, LeafType.BINARY);

        assertThat(restored.isDictionary()).isTrue();
        assertThat(restored.size()).isEqualTo(5);
        assertThat(bytesAt(restored, 0)).containsExactly(utf8("red"));
        assertThat(bytesAt(restored, 1)).containsExactly(utf8("green"));
        assertThat(restored.get(2)).isNull();
        assertThat(bytesAt(restored, 3)).containsExactly(utf8("red"));
        assertThat(bytesAt(restored, 4)).containsExactly(utf8("green"));
    }

    @Test
    void segmentBackedIndicesRoundTripAndEncodeIdenticallyToHeapIndices() {
        MemorySegment red = readOnly(utf8("red"));
        MemorySegment green = readOnly(utf8("green"));
        MemorySegment[] dictEntries = {red, green};
        // Rows: red, green, null, green
        int[] indices = {0, 1, 0, 1};
        BitSet valid = new BitSet(4);
        valid.set(0);
        valid.set(1);
        valid.set(3); // row 2 null
        Validity validity = Validity.of(valid, 4);
        BinaryVector heapForm = BinaryVector.dictionary(dictEntries, IntSequence.of(indices), validity);
        BinaryVector segmentForm = BinaryVector.dictionary(dictEntries, segmentSequence(indices), validity);

        BinaryVector restored = (BinaryVector) roundTrip(segmentForm, LeafType.BINARY);

        assertThat(restored.isDictionary()).isTrue();
        assertThat(bytesAt(restored, 0)).containsExactly(utf8("red"));
        assertThat(bytesAt(restored, 1)).containsExactly(utf8("green"));
        assertThat(restored.get(2)).isNull();
        assertThat(bytesAt(restored, 3)).containsExactly(utf8("green"));
        byte[] segmentIndicesBytes = indicesBufferBytes(ArrowBufferCodec.encode(segmentForm));
        byte[] heapIndicesBytes = indicesBufferBytes(ArrowBufferCodec.encode(heapForm));
        assertThat(segmentIndicesBytes).containsExactly(heapIndicesBytes);
    }

    @Test
    void dictionaryFixedLenBinaryVectorRoundTripsPreservingEncoding() {
        int byteWidth = 4;
        MemorySegment first = readOnly(new byte[] {1, 2, 3, 4});
        MemorySegment second = readOnly(new byte[] {5, 6, 7, 8});
        MemorySegment[] dictEntries = {first, second};
        // Rows: first, second, null, second
        int[] indices = {0, 1, 1, 1};
        BitSet valid = new BitSet(4);
        valid.set(0);
        valid.set(1);
        valid.set(3); // row 2 null
        FixedLenBinaryVector original =
                FixedLenBinaryVector.dictionary(dictEntries, IntSequence.of(indices), byteWidth, Validity.of(valid, 4));

        FixedLenBinaryVector restored = (FixedLenBinaryVector) roundTrip(original, LeafType.FIXED_LEN_BINARY);

        assertThat(restored.isDictionary()).isTrue();
        assertThat(restored.byteWidth()).isEqualTo(byteWidth);
        assertThat(restored.size()).isEqualTo(4);
        assertThat(bytesAt(restored, 0)).containsExactly(1, 2, 3, 4);
        assertThat(bytesAt(restored, 1)).containsExactly(5, 6, 7, 8);
        assertThat(restored.get(2)).isNull();
        assertThat(bytesAt(restored, 3)).containsExactly(5, 6, 7, 8);
    }

    @Test
    void dictionaryInt96VectorRoundTripsPreservingEncoding() {
        MemorySegment first = readOnly(sequentialBytes(12, 0));
        MemorySegment second = readOnly(sequentialBytes(12, 100));
        MemorySegment[] dictEntries = {first, second};
        // Rows: first, null, second, first
        int[] indices = {0, 0, 1, 0};
        BitSet valid = new BitSet(4);
        valid.set(0);
        valid.set(2);
        valid.set(3); // row 1 null
        Int96Vector original = Int96Vector.dictionary(dictEntries, IntSequence.of(indices), Validity.of(valid, 4));

        Int96Vector restored = (Int96Vector) roundTrip(original, LeafType.INT96);

        assertThat(restored.isDictionary()).isTrue();
        assertThat(restored.size()).isEqualTo(4);
        assertThat(bytesAt(restored, 0)).containsExactly(sequentialBytes(12, 0));
        assertThat(restored.get(1)).isNull();
        assertThat(bytesAt(restored, 2)).containsExactly(sequentialBytes(12, 100));
        assertThat(bytesAt(restored, 3)).containsExactly(sequentialBytes(12, 0));
    }

    private static byte[] bytesAt(ColumnVector vector, int row) {
        MemorySegment value = vector.get(row);
        return value.toArray(JAVA_BYTE);
    }

    private static byte[] indicesBufferBytes(EncodedNode node) {
        return node.buffers().stream()
                .filter(buffer -> buffer.role() == BufferRole.DICTIONARY_INDICES)
                .findFirst()
                .orElseThrow()
                .bytes()
                .toArray(JAVA_BYTE);
    }

    /** An {@link IntSequence} over a manually filled little-endian i32 segment. */
    private static IntSequence segmentSequence(int[] values) {
        ValueLayout.OfInt littleEndianI32 = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
        MemorySegment segment = MemorySegment.ofArray(new byte[values.length * Integer.BYTES]);
        for (int i = 0; i < values.length; i++) {
            segment.setAtIndex(littleEndianI32, i, values[i]);
        }
        return IntSequence.ofSegment(segment, values.length);
    }

    private static MemorySegment readOnly(byte[] bytes) {
        return MemorySegment.ofArray(bytes).asReadOnly();
    }

    private static byte[] utf8(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
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
