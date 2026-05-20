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
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.BitSet;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.Encoding;

class BinaryVectorTest {

    @Test
    void rawVectorIsLazy() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = arena.allocate(8);
            BitSet validity = new BitSet(0);
            BinaryVector vec = BinaryVector.raw(page, Encoding.PLAIN, 0, validity);
            assertThat(vec.size()).isZero();
            assertThat(vec.isMaterialized()).isFalse();
        }
    }

    @Test
    void materializeFillsSegmentArray() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = buildPlainBinaryPage(arena, new String[] {"foo", "hello"});
            BitSet validity = new BitSet(2);
            validity.set(0, 2);

            BinaryVector vec = BinaryVector.raw(page, Encoding.PLAIN, 2, validity);
            vec.materialize();

            assertThat(vec.asArray()[0].toArray(JAVA_BYTE)).isEqualTo("foo".getBytes());
            assertThat(vec.asArray()[1].toArray(JAVA_BYTE)).isEqualTo("hello".getBytes());
        }
    }

    @Test
    void materializeIsIdempotent() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = buildPlainBinaryPage(arena, new String[] {"a", "b"});
            BitSet validity = new BitSet(2);
            validity.set(0, 2);

            BinaryVector vec = BinaryVector.raw(page, Encoding.PLAIN, 2, validity);
            vec.materialize();
            vec.materialize();
            assertThat(vec.asArray()).hasSize(2);
        }
    }

    @Test
    void getTriggersMaterialization() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment page = buildPlainBinaryPage(arena, new String[] {"x"});
            BitSet validity = new BitSet(1);
            validity.set(0);

            BinaryVector vec = BinaryVector.raw(page, Encoding.PLAIN, 1, validity);
            assertThat(vec.isMaterialized()).isFalse();

            assertThat(vec.get(0).toArray(JAVA_BYTE)).isEqualTo("x".getBytes());
            assertThat(vec.isMaterialized()).isTrue();
        }
    }

    private static MemorySegment buildPlainBinaryPage(Arena arena, String[] values) {
        int totalSize = 0;
        for (String s : values) {
            totalSize += 4 + s.length();
        }
        byte[] bytes = new byte[totalSize];
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(LITTLE_ENDIAN);
        for (String s : values) {
            buf.putInt(s.length());
            buf.put(s.getBytes());
        }
        MemorySegment page = arena.allocate(totalSize);
        MemorySegment.copy(MemorySegment.ofArray(bytes), 0, page, 0, totalSize);
        return page;
    }
}
