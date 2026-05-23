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

class PlainDoubleDecoderTest {

    @Test
    void decodesFourLittleEndianDoubles() {
        ByteBuffer page = ByteBuffer.allocate(32).order(LITTLE_ENDIAN);
        page.putDouble(0.0);
        page.putDouble(-1.5);
        page.putDouble(Double.MAX_VALUE);
        page.putDouble(Double.MIN_VALUE);
        page.flip();

        PageDecoder<Double> decoder = new PlainDoubleDecoder();
        decoder.load(page, 4);

        assertThat(decoder.next()).isEqualTo(0.0);
        assertThat(decoder.next()).isEqualTo(-1.5);
        assertThat(decoder.next()).isEqualTo(Double.MAX_VALUE);
        assertThat(decoder.next()).isEqualTo(Double.MIN_VALUE);
    }

    @Test
    void skipAdvancesByEightBytes() {
        ByteBuffer page = ByteBuffer.allocate(32).order(LITTLE_ENDIAN);
        page.putDouble(1.1);
        page.putDouble(2.2);
        page.putDouble(3.3);
        page.putDouble(4.4);
        page.flip();

        PageDecoder<Double> decoder = new PlainDoubleDecoder();
        decoder.load(page, 4);
        decoder.skip(2);

        assertThat(decoder.next()).isEqualTo(3.3);
    }

    @Test
    void decodesSpecialDoubleValues() {
        ByteBuffer page = ByteBuffer.allocate(24).order(LITTLE_ENDIAN);
        page.putDouble(Double.NaN);
        page.putDouble(Double.POSITIVE_INFINITY);
        page.putDouble(Double.NEGATIVE_INFINITY);
        page.flip();

        PageDecoder<Double> decoder = new PlainDoubleDecoder();
        decoder.load(page, 3);

        assertThat(decoder.next()).isNaN();
        assertThat(decoder.next()).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(decoder.next()).isEqualTo(Double.NEGATIVE_INFINITY);
    }

    @Test
    void bulkDecodeFillsArray() {
        ByteBuffer page = ByteBuffer.allocate(32).order(LITTLE_ENDIAN);
        page.putDouble(1.0);
        page.putDouble(2.5);
        page.putDouble(Double.POSITIVE_INFINITY);
        page.putDouble(Double.NaN);
        page.flip();

        PageDecoder<Double> decoder = new PlainDoubleDecoder();
        decoder.load(page, 4);

        double[] dst = new double[4];
        decoder.decodeDoubles(4, dst, 0);

        assertThat(dst[0]).isEqualTo(1.0);
        assertThat(dst[1]).isEqualTo(2.5);
        assertThat(dst[2]).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(Double.isNaN(dst[3])).isTrue();
    }
}
