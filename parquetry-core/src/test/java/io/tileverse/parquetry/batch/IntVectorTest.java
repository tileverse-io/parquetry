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

import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.BitSet;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.Encoding;

class IntVectorTest {

    @Test
    void rawVectorIsLazy() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(16);
            page.set(JAVA_INT_UNALIGNED, 0, 10);
            page.set(JAVA_INT_UNALIGNED, 4, 20);
            page.set(JAVA_INT_UNALIGNED, 8, 30);
            page.set(JAVA_INT_UNALIGNED, 12, 40);

            BitSet validity = new BitSet(4);
            validity.set(0, 4);

            IntVector vec = IntVector.raw(page, Encoding.PLAIN, 4, validity);

            assertThat(vec.size()).isEqualTo(4);
            assertThat(vec.isMaterialized()).isFalse();
        }
    }

    @Test
    void materializeFillsArray() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(16);
            for (int i = 0; i < 4; i++) {
                page.set(JAVA_INT_UNALIGNED, i * 4L, (i + 1) * 10);
            }
            BitSet validity = new BitSet(4);
            validity.set(0, 4);

            IntVector vec = IntVector.raw(page, Encoding.PLAIN, 4, validity);
            vec.materialize();

            assertThat(vec.isMaterialized()).isTrue();
            assertThat(vec.get(0)).isEqualTo(10);
            assertThat(vec.get(3)).isEqualTo(40);
            assertThat(vec.asArray()).containsExactly(10, 20, 30, 40);
        }
    }

    @Test
    void materializeIsIdempotent() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(8);
            page.set(JAVA_INT_UNALIGNED, 0, 7);
            page.set(JAVA_INT_UNALIGNED, 4, 8);
            BitSet validity = new BitSet(2);
            validity.set(0, 2);

            IntVector vec = IntVector.raw(page, Encoding.PLAIN, 2, validity);
            vec.materialize();
            vec.materialize();

            assertThat(vec.asArray()).containsExactly(7, 8);
        }
    }

    @Test
    void getTriggersMaterialization() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(4);
            page.set(JAVA_INT_UNALIGNED, 0, 99);
            BitSet validity = new BitSet(1);
            validity.set(0);

            IntVector vec = IntVector.raw(page, Encoding.PLAIN, 1, validity);
            assertThat(vec.isMaterialized()).isFalse();

            assertThat(vec.get(0)).isEqualTo(99);
            assertThat(vec.isMaterialized()).isTrue();
        }
    }
}
