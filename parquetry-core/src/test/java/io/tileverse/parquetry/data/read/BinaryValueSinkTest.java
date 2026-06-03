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
package io.tileverse.parquetry.data.read;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.BitSet;

import org.junit.jupiter.api.Test;

class BinaryValueSinkTest {

    private static MemorySegment seg(String s) {
        return MemorySegment.ofArray(s.getBytes());
    }

    @Test
    void appendsValuesIntoOneBackingWithDenseOffsets() {
        BinaryValueSink sink = new BinaryValueSink();
        MemorySegment src = seg("alphagamma");
        sink.reset(2, 10);
        sink.appendValue(src, 0L, 5);
        sink.appendValue(src, 5L, 5);

        assertThat(sink.count()).isEqualTo(2);
        assertThat(sink.offsets()).containsExactly(0, 5, 10);
        MemorySegment backing = sink.backing();
        assertThat(backing.isReadOnly()).isTrue();
        assertThat(new String(backing.asSlice(0, 5).toArray(JAVA_BYTE))).isEqualTo("alpha");
        assertThat(new String(backing.asSlice(5, 5).toArray(JAVA_BYTE))).isEqualTo("gamma");
    }

    @Test
    void emptyAndZeroLengthValues() {
        BinaryValueSink sink = new BinaryValueSink();
        MemorySegment src = seg("xy");
        sink.reset(3, 2);
        sink.appendValue(src, 0L, 1); // "x"
        sink.appendValue(src, 0L, 0); // empty
        sink.appendValue(src, 1L, 1); // "y"

        assertThat(sink.offsets()).containsExactly(0, 1, 1, 2);
        assertThat(sink.backing().byteSize()).isEqualTo(2);
    }

    @Test
    void spreadsDenseOffsetsToRowPositionsWithNullsAsZeroLengthRuns() {
        int[] denseOffsets = {0, 5, 10};
        BitSet validity = new BitSet(4);
        validity.set(0);
        validity.set(2);

        int[] rowOffsets = BinaryValueSink.spreadOffsets(denseOffsets, validity, 4);

        assertThat(rowOffsets).containsExactly(0, 5, 5, 10, 10);
    }

    @Test
    void spreadIsIdentityWhenAllPresent() {
        int[] denseOffsets = {0, 3, 7};
        BitSet validity = new BitSet(2);
        validity.set(0, 2);

        int[] rowOffsets = BinaryValueSink.spreadOffsets(denseOffsets, validity, 2);

        assertThat(rowOffsets).containsExactly(0, 3, 7);
    }
}
