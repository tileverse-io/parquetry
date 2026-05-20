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
package io.tileverse.parquetry.format.codec;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class CompactProtocolReaderPrimitivesTest {

    @Test
    void zigzagInt32_zero() throws Exception {
        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(new byte[] {0x00}));
        assertThat(r.readI32()).isZero();
    }

    @Test
    void zigzagInt32_minusOne() throws Exception {
        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(new byte[] {0x01}));
        assertThat(r.readI32()).isEqualTo(-1);
    }

    @Test
    void zigzagInt32_positive() throws Exception {
        // 127 -> zigzag = 254 = 0xfe, varint = 0xfe 0x01
        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(new byte[] {(byte) 0xfe, 0x01}));
        assertThat(r.readI32()).isEqualTo(127);
    }

    @Test
    void readBinary_emptyAndNonEmpty() throws Exception {
        CompactProtocolReader emptyReader = new CompactProtocolReader(new ByteArrayInputStream(new byte[] {0x00}));
        MemorySegment empty = emptyReader.readBinary();
        assertThat(empty.byteSize()).isZero();
        assertThat(empty.isReadOnly()).isTrue();

        CompactProtocolReader helloReader =
                new CompactProtocolReader(new ByteArrayInputStream(new byte[] {0x05, 'h', 'e', 'l', 'l', 'o'}));
        MemorySegment hello = helloReader.readBinary();
        assertThat(hello.isReadOnly()).isTrue();
        byte[] expected = "hello".getBytes(StandardCharsets.UTF_8);
        assertThat(hello.toArray(JAVA_BYTE)).isEqualTo(expected);
    }

    @Test
    void readDouble_littleEndian() throws Exception {
        // 1.0 in IEEE 754 little-endian
        byte[] bytes = {0, 0, 0, 0, 0, 0, (byte) 0xf0, 0x3f};
        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(bytes));
        assertThat(r.readDouble()).isEqualTo(1.0);
    }

    @Test
    void readBool_explicit() throws Exception {
        CompactProtocolReader rTrue = new CompactProtocolReader(new ByteArrayInputStream(new byte[] {0x01}));
        assertThat(rTrue.readBool()).isTrue();
        CompactProtocolReader rFalse = new CompactProtocolReader(new ByteArrayInputStream(new byte[] {0x02}));
        assertThat(rFalse.readBool()).isFalse();
    }
}
