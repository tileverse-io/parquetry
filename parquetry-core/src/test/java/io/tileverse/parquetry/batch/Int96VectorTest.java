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

class Int96VectorTest {

    private static final int INT96_BYTES = 12;

    @Test
    void rawVectorIsLazy() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(0);
            BitSet validity = new BitSet(0);
            Int96Vector vec = Int96Vector.raw(page, Encoding.PLAIN, 0, validity);
            assertThat(vec.size()).isZero();
            assertThat(vec.isMaterialized()).isFalse();
        }
    }

    @Test
    void materializeFillsSegmentArray() {
        try (Arena arena = Arena.ofConfined()) {
            byte[] raw = buildInt96Page(3);
            MemorySegment page = arena.allocate(raw.length);
            MemorySegment.copy(MemorySegment.ofArray(raw), 0, page, 0, raw.length);
            BitSet validity = new BitSet(3);
            validity.set(0, 3);

            Int96Vector vec = Int96Vector.raw(page, Encoding.PLAIN, 3, validity);
            vec.materialize();

            assertThat(vec.asArray()[0].toArray(JAVA_BYTE)).isEqualTo(valueAt(raw, 0));
            assertThat(vec.asArray()[1].toArray(JAVA_BYTE)).isEqualTo(valueAt(raw, 1));
            assertThat(vec.asArray()[2].toArray(JAVA_BYTE)).isEqualTo(valueAt(raw, 2));
        }
    }

    @Test
    void materializeIsIdempotent() {
        try (Arena arena = Arena.ofConfined()) {
            byte[] raw = buildInt96Page(2);
            MemorySegment page = arena.allocate(raw.length);
            MemorySegment.copy(MemorySegment.ofArray(raw), 0, page, 0, raw.length);
            BitSet validity = new BitSet(2);
            validity.set(0, 2);

            Int96Vector vec = Int96Vector.raw(page, Encoding.PLAIN, 2, validity);
            vec.materialize();
            vec.materialize();
            assertThat(vec.asArray()).hasSize(2);
        }
    }

    @Test
    void getTriggersMaterialization() {
        try (Arena arena = Arena.ofConfined()) {
            byte[] raw = buildInt96Page(1);
            MemorySegment page = arena.allocate(raw.length);
            MemorySegment.copy(MemorySegment.ofArray(raw), 0, page, 0, raw.length);
            BitSet validity = new BitSet(1);
            validity.set(0);

            Int96Vector vec = Int96Vector.raw(page, Encoding.PLAIN, 1, validity);
            assertThat(vec.isMaterialized()).isFalse();

            assertThat(vec.get(0).toArray(JAVA_BYTE)).isEqualTo(valueAt(raw, 0));
            assertThat(vec.isMaterialized()).isTrue();
        }
    }

    /** Builds a PLAIN INT96 page with {@code count} 12-byte values. Each byte is set to its index. */
    private static byte[] buildInt96Page(int count) {
        byte[] page = new byte[count * INT96_BYTES];
        for (int i = 0; i < page.length; i++) {
            page[i] = (byte) i;
        }
        return page;
    }

    /** Extracts the expected 12 bytes for value at the given index from the raw page. */
    private static byte[] valueAt(byte[] raw, int index) {
        byte[] value = new byte[INT96_BYTES];
        System.arraycopy(raw, index * INT96_BYTES, value, 0, INT96_BYTES);
        return value;
    }
}
