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
package io.tileverse.parquetry.page.plain;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.page.PageDecoder;

class PlainInt32DecoderTest {

    @Test
    void decodesFourLittleEndianInts() {
        ByteBuffer page = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        page.putInt(0);
        page.putInt(-1);
        page.putInt(Integer.MAX_VALUE);
        page.putInt(42);
        page.flip();

        PageDecoder<Integer> decoder = new PlainInt32Decoder();
        decoder.load(page, 4);

        assertThat(decoder.next()).isZero();
        assertThat(decoder.next()).isEqualTo(-1);
        assertThat(decoder.next()).isEqualTo(Integer.MAX_VALUE);
        assertThat(decoder.next()).isEqualTo(42);
    }

    @Test
    void skipAdvancesByByteWidth() {
        ByteBuffer page = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN);
        page.putInt(10);
        page.putInt(20);
        page.putInt(30);
        page.putInt(40);
        page.flip();

        PageDecoder<Integer> decoder = new PlainInt32Decoder();
        decoder.load(page, 4);
        decoder.skip(2);

        assertThat(decoder.next()).isEqualTo(30);
    }

    @Test
    void skipThenNextThenSkipAgain() {
        ByteBuffer page = ByteBuffer.allocate(20).order(ByteOrder.LITTLE_ENDIAN);
        page.putInt(1);
        page.putInt(2);
        page.putInt(3);
        page.putInt(4);
        page.putInt(5);
        page.flip();

        PageDecoder<Integer> decoder = new PlainInt32Decoder();
        decoder.load(page, 5);
        decoder.skip(1);
        assertThat(decoder.next()).isEqualTo(2);
        decoder.skip(1);
        assertThat(decoder.next()).isEqualTo(4);
    }
}
