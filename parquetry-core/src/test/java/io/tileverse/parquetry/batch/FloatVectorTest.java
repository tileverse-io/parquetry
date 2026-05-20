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

import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.BitSet;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.Encoding;

class FloatVectorTest {

    @Test
    void rawVectorIsLazy() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(16);
            page.set(JAVA_FLOAT_UNALIGNED, 0, 1.0f);
            page.set(JAVA_FLOAT_UNALIGNED, 4, 2.0f);
            page.set(JAVA_FLOAT_UNALIGNED, 8, 3.0f);
            page.set(JAVA_FLOAT_UNALIGNED, 12, 4.0f);

            BitSet validity = new BitSet(4);
            validity.set(0, 4);

            FloatVector vec = FloatVector.raw(page, Encoding.PLAIN, 4, validity);

            assertThat(vec.size()).isEqualTo(4);
            assertThat(vec.isMaterialized()).isFalse();
        }
    }

    @Test
    void materializeFillsArray() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(16);
            page.set(JAVA_FLOAT_UNALIGNED, 0, 1.0f);
            page.set(JAVA_FLOAT_UNALIGNED, 4, 2.5f);
            page.set(JAVA_FLOAT_UNALIGNED, 8, Float.NEGATIVE_INFINITY);
            page.set(JAVA_FLOAT_UNALIGNED, 12, Float.NaN);

            BitSet validity = new BitSet(4);
            validity.set(0, 4);

            FloatVector vec = FloatVector.raw(page, Encoding.PLAIN, 4, validity);
            vec.materialize();

            assertThat(vec.isMaterialized()).isTrue();
            assertThat(vec.get(0)).isEqualTo(1.0f);
            assertThat(vec.get(1)).isEqualTo(2.5f);
            assertThat(vec.get(2)).isEqualTo(Float.NEGATIVE_INFINITY);
            assertThat(Float.isNaN(vec.get(3))).isTrue();
            assertThat(vec.asArray()).hasSize(4);
        }
    }

    @Test
    void materializeIsIdempotent() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(8);
            page.set(JAVA_FLOAT_UNALIGNED, 0, 1.0f);
            page.set(JAVA_FLOAT_UNALIGNED, 4, 2.0f);
            BitSet validity = new BitSet(2);
            validity.set(0, 2);

            FloatVector vec = FloatVector.raw(page, Encoding.PLAIN, 2, validity);
            vec.materialize();
            vec.materialize();

            assertThat(vec.asArray()).containsExactly(1.0f, 2.0f);
        }
    }

    @Test
    void getTriggersMaterialization() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(4);
            page.set(JAVA_FLOAT_UNALIGNED, 0, 99.0f);
            BitSet validity = new BitSet(1);
            validity.set(0);

            FloatVector vec = FloatVector.raw(page, Encoding.PLAIN, 1, validity);
            assertThat(vec.isMaterialized()).isFalse();

            assertThat(vec.get(0)).isEqualTo(99.0f);
            assertThat(vec.isMaterialized()).isTrue();
        }
    }
}
