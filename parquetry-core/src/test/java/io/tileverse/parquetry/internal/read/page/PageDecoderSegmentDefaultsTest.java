/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.internal.read.page;

import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;
import static io.tileverse.parquetry.format.ParquetLayouts.FLOAT;
import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Test;

/** The boxed {@code next()} fallback behind the segment-write bulk defaults of {@link PageDecoder}. */
class PageDecoderSegmentDefaultsTest {

    @Test
    void defaultDecodeIntsWritesAtElementIndex() {
        PageDecoder<Integer> decoder = sequence(10, 20, 30);
        MemorySegment dst = MemorySegment.ofArray(new byte[4 * Integer.BYTES]);
        dst.setAtIndex(INT32, 0, -1); // sentinel - must be left alone

        decoder.decodeInts(3, dst, 1);

        assertThat(dst.getAtIndex(INT32, 0)).isEqualTo(-1);
        assertThat(dst.getAtIndex(INT32, 1)).isEqualTo(10);
        assertThat(dst.getAtIndex(INT32, 2)).isEqualTo(20);
        assertThat(dst.getAtIndex(INT32, 3)).isEqualTo(30);
    }

    @Test
    void defaultDecodeLongsWritesAtElementIndex() {
        PageDecoder<Long> decoder = sequence(10L, 20L);
        MemorySegment dst = MemorySegment.ofArray(new byte[3 * Long.BYTES]);

        decoder.decodeLongs(2, dst, 1);

        assertThat(dst.getAtIndex(INT64, 1)).isEqualTo(10L);
        assertThat(dst.getAtIndex(INT64, 2)).isEqualTo(20L);
    }

    @Test
    void defaultDecodeFloatsWritesAtElementIndex() {
        PageDecoder<Float> decoder = sequence(1.5f, -2.5f);
        MemorySegment dst = MemorySegment.ofArray(new byte[3 * Float.BYTES]);
        dst.setAtIndex(FLOAT, 0, -1.0f); // sentinel - must be left alone

        decoder.decodeFloats(2, dst, 1);

        assertThat(dst.getAtIndex(FLOAT, 0)).isEqualTo(-1.0f);
        assertThat(dst.getAtIndex(FLOAT, 1)).isEqualTo(1.5f);
        assertThat(dst.getAtIndex(FLOAT, 2)).isEqualTo(-2.5f);
    }

    @Test
    void defaultDecodeDoublesWritesAtElementIndex() {
        PageDecoder<Double> decoder = sequence(1.5d, -2.5d);
        MemorySegment dst = MemorySegment.ofArray(new byte[3 * Double.BYTES]);
        dst.setAtIndex(DOUBLE, 0, -1.0d); // sentinel - must be left alone

        decoder.decodeDoubles(2, dst, 1);

        assertThat(dst.getAtIndex(DOUBLE, 0)).isEqualTo(-1.0d);
        assertThat(dst.getAtIndex(DOUBLE, 1)).isEqualTo(1.5d);
        assertThat(dst.getAtIndex(DOUBLE, 2)).isEqualTo(-2.5d);
    }

    /** A minimal decoder that hands out the given values from {@code next()}, leaving every default in place. */
    @SafeVarargs
    private static <T> PageDecoder<T> sequence(T... values) {
        return new PageDecoder<>() {
            private int cursor;

            @Override
            public void load(MemorySegment page, int valueCount) {
                // nothing to load; the values come from the captured array
            }

            @Override
            public T next() {
                return values[cursor++];
            }

            @Override
            public void skip(int n) {
                cursor += n;
            }
        };
    }
}
