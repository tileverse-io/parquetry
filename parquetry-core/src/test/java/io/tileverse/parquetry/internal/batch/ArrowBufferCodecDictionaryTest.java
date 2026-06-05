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
        BinaryVector original = BinaryVector.dictionary(dictEntries, indices, Validity.of(valid, 5));

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
                FixedLenBinaryVector.dictionary(dictEntries, indices, byteWidth, Validity.of(valid, 4));

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
        Int96Vector original = Int96Vector.dictionary(dictEntries, indices, Validity.of(valid, 4));

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
