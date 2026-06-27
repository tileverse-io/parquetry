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
package io.tileverse.parquetry.internal.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.Levels;
import io.tileverse.parquetry.internal.read.page.LevelDecoder;
import io.tileverse.parquetry.io.SegmentPool;

class LevelScratchTest {

    @Test
    void reusesItsBufferAcrossDecodesAndReleasesOnClose() {
        SegmentPool pool = SegmentPool.create();
        LevelScratch scratch = new LevelScratch(TestDecodeBuffers.ample(pool));

        // RLE 3 zeros, bit-packed 8 ones, RLE 2 zeros -> [0,0,0,1,1,1,1,1,1,1,1,0,0]
        LevelDecoder decoder = new LevelDecoder(1);
        decoder.load(MemorySegment.ofArray(new byte[] {6, 0, 3, (byte) 0xff, 4, 0}));
        Levels first = scratch.decode(decoder, 13);
        assertThat(first.size()).isEqualTo(13);
        assertThat(first.get(0)).isZero();
        assertThat(first.get(3)).isEqualTo(1);
        assertThat(first.get(12)).isZero();
        long borrowsAfterFirst = pool.stats().totalBorrows();

        // RLE run of 10 ones: header (10 << 1) = 0x14, value byte 1.
        decoder.load(MemorySegment.ofArray(new byte[] {0x14, 0x01}));
        Levels second = scratch.decode(decoder, 10);
        assertThat(second.size()).isEqualTo(10);
        assertThat(second.get(9)).isEqualTo(1);
        assertThat(pool.stats().totalBorrows())
                .as("a page that fits the existing capacity borrows nothing new")
                .isEqualTo(borrowsAfterFirst);

        scratch.close();
        scratch.close(); // idempotent
        assertThat(pool.stats().outstandingBorrows()).isZero();
    }

    @Test
    void growsWhenAPageExceedsCapacity() {
        SegmentPool pool = SegmentPool.create();
        LevelScratch scratch = new LevelScratch(TestDecodeBuffers.ample(pool));

        LevelDecoder decoder = new LevelDecoder(1);
        decoder.load(MemorySegment.ofArray(new byte[] {0x14, 0x01})); // RLE 10 ones
        scratch.decode(decoder, 10);
        long borrowsAfterFirst = pool.stats().totalBorrows();

        // RLE run of 5000 ones: header varint (5000 << 1) = 10000 -> bytes 0x90 0x4E; value byte 1.
        // 5000 levels need 20000 bytes, above the initial capacity -> the scratch must grow.
        decoder.load(MemorySegment.ofArray(new byte[] {(byte) 0x90, 0x4E, 0x01}));
        Levels grown = scratch.decode(decoder, 5000);
        assertThat(grown.size()).isEqualTo(5000);
        assertThat(grown.get(0)).isEqualTo(1);
        assertThat(grown.get(4999)).isEqualTo(1);
        assertThat(pool.stats().totalBorrows()).as("growth reacquires once").isEqualTo(borrowsAfterFirst + 1);
        assertThat(pool.stats().outstandingBorrows())
                .as("the outgrown buffer went back to the pool")
                .isEqualTo(1);

        scratch.close();
        assertThat(pool.stats().outstandingBorrows()).isZero();
    }
}
