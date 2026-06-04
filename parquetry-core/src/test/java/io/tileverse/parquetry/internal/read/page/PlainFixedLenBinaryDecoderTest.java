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
package io.tileverse.parquetry.internal.read.page;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class PlainFixedLenBinaryDecoderTest {

    @Test
    void decodesThreeFixedLengthEntries() {
        byte[] aaa = "aaaaa".getBytes(StandardCharsets.UTF_8);
        byte[] bbb = "bbbbb".getBytes(StandardCharsets.UTF_8);
        byte[] ccc = "ccccc".getBytes(StandardCharsets.UTF_8);

        ByteBuffer page = ByteBuffer.allocate(15);
        page.put(aaa);
        page.put(bbb);
        page.put(ccc);
        page.flip();

        PageDecoder<MemorySegment> decoder = new PlainFixedLenBinaryDecoder(5);
        decoder.load(MemorySegment.ofBuffer(page), 3);

        assertThat(decoder.next().toArray(JAVA_BYTE)).isEqualTo(aaa);
        assertThat(decoder.next().toArray(JAVA_BYTE)).isEqualTo(bbb);
        assertThat(decoder.next().toArray(JAVA_BYTE)).isEqualTo(ccc);
    }

    @Test
    void slicesAreReadOnly() {
        ByteBuffer page = ByteBuffer.allocate(5);
        page.put("hello".getBytes(StandardCharsets.UTF_8));
        page.flip();

        PageDecoder<MemorySegment> decoder = new PlainFixedLenBinaryDecoder(5);
        decoder.load(MemorySegment.ofBuffer(page), 1);

        MemorySegment slice = decoder.next();
        assertThat(slice.isReadOnly()).isTrue();
    }

    @Test
    void sliceRemainingEqualsLength() {
        ByteBuffer page = ByteBuffer.allocate(10);
        page.put("0123456789".getBytes(StandardCharsets.UTF_8));
        page.flip();

        PageDecoder<MemorySegment> decoder = new PlainFixedLenBinaryDecoder(5);
        decoder.load(MemorySegment.ofBuffer(page), 2);

        assertThat(decoder.next().byteSize()).isEqualTo(5);
        assertThat(decoder.next().byteSize()).isEqualTo(5);
    }

    @Test
    void skipAdvancesByNTimesLength() {
        byte[] first = "AAAAA".getBytes(StandardCharsets.UTF_8);
        byte[] second = "BBBBB".getBytes(StandardCharsets.UTF_8);
        byte[] third = "CCCCC".getBytes(StandardCharsets.UTF_8);

        ByteBuffer page = ByteBuffer.allocate(15);
        page.put(first);
        page.put(second);
        page.put(third);
        page.flip();

        PageDecoder<MemorySegment> decoder = new PlainFixedLenBinaryDecoder(5);
        decoder.load(MemorySegment.ofBuffer(page), 3);
        decoder.skip(2);

        assertThat(decoder.next().toArray(JAVA_BYTE)).isEqualTo(third);
    }

    @Test
    void rejectsNegativeLength() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new PlainFixedLenBinaryDecoder(-1))
                .withMessageContaining("non-negative");
    }

    @Test
    void acceptsZeroLength() {
        // Zero-length fixed-width columns are degenerate but allowed by the spec
        ByteBuffer page = ByteBuffer.allocate(0);
        page.flip();

        PageDecoder<MemorySegment> decoder = new PlainFixedLenBinaryDecoder(0);
        decoder.load(MemorySegment.ofBuffer(page), 3);

        assertThat(decoder.next().byteSize()).isZero();
        assertThat(decoder.next().byteSize()).isZero();
        assertThat(decoder.next().byteSize()).isZero();
    }

    @Test
    void bulkDecodeFillsFixedLenSegments() {
        // 3 values of fixed length 4 bytes each.
        ByteBuffer page = ByteBuffer.allocate(12);
        page.put("AAAA".getBytes());
        page.put("BBBB".getBytes());
        page.put("CCCC".getBytes());
        page.flip();

        PageDecoder<MemorySegment> decoder = new PlainFixedLenBinaryDecoder(4);
        decoder.load(MemorySegment.ofBuffer(page), 3);

        MemorySegment[] dst = new MemorySegment[3];
        decoder.decodeBinary(3, dst, 0);

        assertThat(dst[0].toArray(JAVA_BYTE)).isEqualTo("AAAA".getBytes());
        assertThat(dst[1].toArray(JAVA_BYTE)).isEqualTo("BBBB".getBytes());
        assertThat(dst[2].toArray(JAVA_BYTE)).isEqualTo("CCCC".getBytes());
    }
}
