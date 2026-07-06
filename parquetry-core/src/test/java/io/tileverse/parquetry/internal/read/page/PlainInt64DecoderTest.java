/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.ParquetLayouts;

class PlainInt64DecoderTest {

    @Test
    void decodesFourLittleEndianLongs() {
        ByteBuffer page = ByteBuffer.allocate(32).order(LITTLE_ENDIAN);
        page.putLong(0L);
        page.putLong(-1L);
        page.putLong(Long.MAX_VALUE);
        page.putLong(Long.MIN_VALUE);
        page.flip();

        PageDecoder<Long> decoder = new PlainInt64Decoder();
        decoder.load(MemorySegment.ofBuffer(page), 4);

        assertThat(decoder.next()).isZero();
        assertThat(decoder.next()).isEqualTo(-1L);
        assertThat(decoder.next()).isEqualTo(Long.MAX_VALUE);
        assertThat(decoder.next()).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    void skipAdvancesByEightBytes() {
        ByteBuffer page = ByteBuffer.allocate(32).order(LITTLE_ENDIAN);
        page.putLong(100L);
        page.putLong(200L);
        page.putLong(300L);
        page.putLong(400L);
        page.flip();

        PageDecoder<Long> decoder = new PlainInt64Decoder();
        decoder.load(MemorySegment.ofBuffer(page), 4);
        decoder.skip(2);

        assertThat(decoder.next()).isEqualTo(300L);
    }

    @Test
    void bulkDecodeFillsArray() {
        ByteBuffer page = ByteBuffer.allocate(32).order(LITTLE_ENDIAN);
        page.putLong(100L);
        page.putLong(200L);
        page.putLong(300L);
        page.putLong(400L);
        page.flip();

        PageDecoder<Long> decoder = new PlainInt64Decoder();
        decoder.load(MemorySegment.ofBuffer(page), 4);

        long[] dst = new long[5];
        dst[0] = -1L;
        decoder.decodeLongs(4, dst, 1);

        assertThat(dst).containsExactly(-1L, 100L, 200L, 300L, 400L);
    }

    @Test
    void bulkDecodeIntoSegmentCopiesBytes() {
        ByteBuffer page = ByteBuffer.allocate(32).order(LITTLE_ENDIAN);
        page.putLong(100L);
        page.putLong(200L);
        page.putLong(300L);
        page.putLong(400L);
        page.flip();

        PageDecoder<Long> decoder = new PlainInt64Decoder();
        decoder.load(MemorySegment.ofBuffer(page), 4);

        MemorySegment dst = MemorySegment.ofArray(new byte[5 * Long.BYTES]);
        dst.setAtIndex(ParquetLayouts.INT64, 0, -1L); // sentinel - must be left alone
        decoder.decodeLongs(4, dst, 1);

        assertThat(dst.getAtIndex(ParquetLayouts.INT64, 0)).isEqualTo(-1L);
        assertThat(dst.getAtIndex(ParquetLayouts.INT64, 1)).isEqualTo(100L);
        assertThat(dst.getAtIndex(ParquetLayouts.INT64, 4)).isEqualTo(400L);
    }

    @Test
    void bulkDecodeIntoSegmentAfterPartialNext() {
        ByteBuffer page = ByteBuffer.allocate(32).order(LITTLE_ENDIAN);
        page.putLong(100L);
        page.putLong(200L);
        page.putLong(300L);
        page.putLong(400L);
        page.flip();

        PageDecoder<Long> decoder = new PlainInt64Decoder();
        decoder.load(MemorySegment.ofBuffer(page), 4);
        assertThat(decoder.next()).isEqualTo(100L);

        MemorySegment dst = MemorySegment.ofArray(new byte[2 * Long.BYTES]);
        decoder.decodeLongs(2, dst, 0);

        assertThat(dst.getAtIndex(ParquetLayouts.INT64, 0)).isEqualTo(200L);
        assertThat(dst.getAtIndex(ParquetLayouts.INT64, 1)).isEqualTo(300L);
        assertThat(decoder.next()).isEqualTo(400L);
    }
}
