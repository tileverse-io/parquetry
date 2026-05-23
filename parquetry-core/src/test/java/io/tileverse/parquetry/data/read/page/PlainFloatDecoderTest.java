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
package io.tileverse.parquetry.data.read.page;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

class PlainFloatDecoderTest {

    @Test
    void decodesFourLittleEndianFloats() {
        ByteBuffer page = ByteBuffer.allocate(16).order(LITTLE_ENDIAN);
        page.putFloat(0.0f);
        page.putFloat(-1.5f);
        page.putFloat(Float.MAX_VALUE);
        page.putFloat(Float.MIN_VALUE);
        page.flip();

        PageDecoder<Float> decoder = new PlainFloatDecoder();
        decoder.load(page, 4);

        assertThat(decoder.next()).isEqualTo(0.0f);
        assertThat(decoder.next()).isEqualTo(-1.5f);
        assertThat(decoder.next()).isEqualTo(Float.MAX_VALUE);
        assertThat(decoder.next()).isEqualTo(Float.MIN_VALUE);
    }

    @Test
    void skipAdvancesByFourBytes() {
        ByteBuffer page = ByteBuffer.allocate(16).order(LITTLE_ENDIAN);
        page.putFloat(1.1f);
        page.putFloat(2.2f);
        page.putFloat(3.3f);
        page.putFloat(4.4f);
        page.flip();

        PageDecoder<Float> decoder = new PlainFloatDecoder();
        decoder.load(page, 4);
        decoder.skip(2);

        assertThat(decoder.next()).isEqualTo(3.3f);
    }

    @Test
    void decodesSpecialFloatValues() {
        ByteBuffer page = ByteBuffer.allocate(12).order(LITTLE_ENDIAN);
        page.putFloat(Float.NaN);
        page.putFloat(Float.POSITIVE_INFINITY);
        page.putFloat(Float.NEGATIVE_INFINITY);
        page.flip();

        PageDecoder<Float> decoder = new PlainFloatDecoder();
        decoder.load(page, 3);

        assertThat(decoder.next()).isNaN();
        assertThat(decoder.next()).isEqualTo(Float.POSITIVE_INFINITY);
        assertThat(decoder.next()).isEqualTo(Float.NEGATIVE_INFINITY);
    }

    @Test
    void bulkDecodeFillsArray() {
        ByteBuffer page = ByteBuffer.allocate(16).order(LITTLE_ENDIAN);
        page.putFloat(1.0f);
        page.putFloat(2.5f);
        page.putFloat(Float.NEGATIVE_INFINITY);
        page.putFloat(Float.NaN);
        page.flip();

        PageDecoder<Float> decoder = new PlainFloatDecoder();
        decoder.load(page, 4);

        float[] dst = new float[4];
        decoder.decodeFloats(4, dst, 0);

        assertThat(dst[0]).isEqualTo(1.0f);
        assertThat(dst[1]).isEqualTo(2.5f);
        assertThat(dst[2]).isEqualTo(Float.NEGATIVE_INFINITY);
        assertThat(Float.isNaN(dst[3])).isTrue();
    }
}
