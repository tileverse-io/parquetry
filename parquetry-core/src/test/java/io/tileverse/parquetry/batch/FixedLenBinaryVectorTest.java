/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.batch;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.BitSet;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.Encoding;

class FixedLenBinaryVectorTest {

    @Test
    void rawVectorIsLazy() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(0);
            BitSet validity = new BitSet(0);
            FixedLenBinaryVector vec = FixedLenBinaryVector.raw(page, Encoding.PLAIN, 0, 4, validity);
            assertThat(vec.size()).isZero();
            assertThat(vec.byteWidth()).isEqualTo(4);
            assertThat(vec.isMaterialized()).isFalse();
        }
    }

    @Test
    void materializeFillsFixedLenSegments() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(12);
            MemorySegment.copy(MemorySegment.ofArray("AAAABBBBCCCC".getBytes()), 0, page, 0, 12);
            BitSet validity = new BitSet(3);
            validity.set(0, 3);

            FixedLenBinaryVector vec = FixedLenBinaryVector.raw(page, Encoding.PLAIN, 3, 4, validity);
            vec.materialize();

            assertThat(vec.byteWidth()).isEqualTo(4);
            assertThat(vec.asArray()[0].toArray(JAVA_BYTE)).isEqualTo("AAAA".getBytes());
            assertThat(vec.asArray()[1].toArray(JAVA_BYTE)).isEqualTo("BBBB".getBytes());
            assertThat(vec.asArray()[2].toArray(JAVA_BYTE)).isEqualTo("CCCC".getBytes());
        }
    }

    @Test
    void materializeIsIdempotent() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(8);
            MemorySegment.copy(MemorySegment.ofArray("AAAABBBB".getBytes()), 0, page, 0, 8);
            BitSet validity = new BitSet(2);
            validity.set(0, 2);

            FixedLenBinaryVector vec = FixedLenBinaryVector.raw(page, Encoding.PLAIN, 2, 4, validity);
            vec.materialize();
            vec.materialize();
            assertThat(vec.asArray()).hasSize(2);
        }
    }

    @Test
    void getTriggersMaterialization() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(4);
            MemorySegment.copy(MemorySegment.ofArray("XYZW".getBytes()), 0, page, 0, 4);
            BitSet validity = new BitSet(1);
            validity.set(0);

            FixedLenBinaryVector vec = FixedLenBinaryVector.raw(page, Encoding.PLAIN, 1, 4, validity);
            assertThat(vec.isMaterialized()).isFalse();

            assertThat(vec.get(0).toArray(JAVA_BYTE)).isEqualTo("XYZW".getBytes());
            assertThat(vec.isMaterialized()).isTrue();
        }
    }
}
