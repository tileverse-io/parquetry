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

class BooleanVectorTest {

    @Test
    void rawVectorIsLazy() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(1);
            page.set(JAVA_BYTE, 0, (byte) 0b01010101);
            BitSet validity = new BitSet(4);
            validity.set(0, 4);

            BooleanVector vec = BooleanVector.raw(page, Encoding.PLAIN, 4, validity);

            assertThat(vec.size()).isEqualTo(4);
            assertThat(vec.isMaterialized()).isFalse();
        }
    }

    @Test
    void materializeFillsArrayFromPlainEncoding() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(1);
            page.set(JAVA_BYTE, 0, (byte) 0b01010101); // LSB-first: true, false, true, false, ...
            BitSet validity = new BitSet(4);
            validity.set(0, 4);

            BooleanVector vec = BooleanVector.raw(page, Encoding.PLAIN, 4, validity);
            vec.materialize();

            assertThat(vec.isMaterialized()).isTrue();
            assertThat(vec.asArray()).containsExactly(true, false, true, false);
        }
    }

    @Test
    void materializeIsIdempotent() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(1);
            page.set(JAVA_BYTE, 0, (byte) 0b00000011); // true, true, then false padding
            BitSet validity = new BitSet(2);
            validity.set(0, 2);

            BooleanVector vec = BooleanVector.raw(page, Encoding.PLAIN, 2, validity);
            vec.materialize();
            vec.materialize();

            assertThat(vec.asArray()).containsExactly(true, true);
        }
    }

    @Test
    void getTriggersMaterialization() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(1);
            page.set(JAVA_BYTE, 0, (byte) 0b00000001); // true
            BitSet validity = new BitSet(1);
            validity.set(0);

            BooleanVector vec = BooleanVector.raw(page, Encoding.PLAIN, 1, validity);
            assertThat(vec.isMaterialized()).isFalse();

            assertThat(vec.get(0)).isTrue();
            assertThat(vec.isMaterialized()).isTrue();
        }
    }
}
