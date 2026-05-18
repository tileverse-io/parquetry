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

class PlainInt64DecoderTest {

    @Test
    void decodesFourLittleEndianLongs() {
        ByteBuffer page = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN);
        page.putLong(0L);
        page.putLong(-1L);
        page.putLong(Long.MAX_VALUE);
        page.putLong(Long.MIN_VALUE);
        page.flip();

        PageDecoder<Long> decoder = new PlainInt64Decoder();
        decoder.load(page, 4);

        assertThat(decoder.next()).isZero();
        assertThat(decoder.next()).isEqualTo(-1L);
        assertThat(decoder.next()).isEqualTo(Long.MAX_VALUE);
        assertThat(decoder.next()).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    void skipAdvancesByEightBytes() {
        ByteBuffer page = ByteBuffer.allocate(32).order(ByteOrder.LITTLE_ENDIAN);
        page.putLong(100L);
        page.putLong(200L);
        page.putLong(300L);
        page.putLong(400L);
        page.flip();

        PageDecoder<Long> decoder = new PlainInt64Decoder();
        decoder.load(page, 4);
        decoder.skip(2);

        assertThat(decoder.next()).isEqualTo(300L);
    }
}
