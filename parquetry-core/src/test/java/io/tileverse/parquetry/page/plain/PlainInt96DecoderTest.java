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

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.page.PageDecoder;

class PlainInt96DecoderTest {

    private static final int INT96_BYTES = 12;

    @Test
    void decodesThreeInt96ValuesAsSlices() {
        // Three distinct 12-byte values
        byte[] value0 = new byte[INT96_BYTES];
        byte[] value1 = new byte[INT96_BYTES];
        byte[] value2 = new byte[INT96_BYTES];
        for (int i = 0; i < INT96_BYTES; i++) {
            value0[i] = (byte) i;
            value1[i] = (byte) (i + 16);
            value2[i] = (byte) (i + 32);
        }

        ByteBuffer page = ByteBuffer.allocate(3 * INT96_BYTES);
        page.put(value0);
        page.put(value1);
        page.put(value2);
        page.flip();

        PageDecoder<ByteBuffer> decoder = new PlainInt96Decoder();
        decoder.load(page, 3);

        ByteBuffer result0 = decoder.next();
        assertThat(result0.remaining()).isEqualTo(INT96_BYTES);
        assertThat(result0).isEqualTo(ByteBuffer.wrap(value0));

        ByteBuffer result1 = decoder.next();
        assertThat(result1.remaining()).isEqualTo(INT96_BYTES);
        assertThat(result1).isEqualTo(ByteBuffer.wrap(value1));

        ByteBuffer result2 = decoder.next();
        assertThat(result2.remaining()).isEqualTo(INT96_BYTES);
        assertThat(result2).isEqualTo(ByteBuffer.wrap(value2));
    }

    @Test
    void sliceIsReadOnly() {
        // Must write INT96_BYTES before flipping so the buffer's limit covers all 12 bytes.
        ByteBuffer page = ByteBuffer.allocate(INT96_BYTES);
        page.put(new byte[INT96_BYTES]);
        page.flip();

        PageDecoder<ByteBuffer> decoder = new PlainInt96Decoder();
        decoder.load(page, 1);

        ByteBuffer slice = decoder.next();
        assertThat(slice.isReadOnly()).isTrue();
    }

    @Test
    void skipAdvancesByTwelveBytes() {
        byte[] skipped = new byte[INT96_BYTES];
        byte[] kept = new byte[INT96_BYTES];
        for (int i = 0; i < INT96_BYTES; i++) {
            skipped[i] = (byte) 0x00;
            kept[i] = (byte) (i + 1);
        }

        ByteBuffer page = ByteBuffer.allocate(3 * INT96_BYTES);
        page.put(skipped);
        page.put(skipped);
        page.put(kept);
        page.flip();

        PageDecoder<ByteBuffer> decoder = new PlainInt96Decoder();
        decoder.load(page, 3);
        decoder.skip(2);

        ByteBuffer result = decoder.next();
        assertThat(result).isEqualTo(ByteBuffer.wrap(kept));
    }
}
