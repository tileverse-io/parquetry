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
package io.tileverse.parquetry.format.codec;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;

import org.junit.jupiter.api.Test;

class CompactProtocolReaderFramingTest {

    @Test
    void readFieldHeader_smallDelta() throws Exception {
        // type=I32 (0x05), delta=1 -> single byte 0x15
        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(new byte[] {0x15}));
        FieldHeader header = r.readFieldHeader(0);
        assertThat(header.fieldId()).isEqualTo((short) 1);
        assertThat(header.type()).isEqualTo(CompactType.I32);
    }

    @Test
    void readFieldHeader_largeFieldId() throws Exception {
        // type=I32 (0x05), delta=0 -> 0x05, then zigzag varint i16 fieldId
        // fieldId 1000 -> zigzag = 2000 = 0xd0 0x0f
        CompactProtocolReader r =
                new CompactProtocolReader(new ByteArrayInputStream(new byte[] {0x05, (byte) 0xd0, 0x0f}));
        FieldHeader header = r.readFieldHeader(0);
        assertThat(header.fieldId()).isEqualTo((short) 1000);
        assertThat(header.type()).isEqualTo(CompactType.I32);
    }

    @Test
    void readStop() throws Exception {
        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(new byte[] {0x00}));
        FieldHeader header = r.readFieldHeader(0);
        assertThat(header.isStop()).isTrue();
    }

    @Test
    void readListHeader_smallList() throws Exception {
        // list of i32, size 3: 0x35 (size<<4 | elementType=I32(0x05))
        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(new byte[] {0x35}));
        CompactProtocolReader.ListHeader lh = r.readListHeader();
        assertThat(lh.size()).isEqualTo(3);
        assertThat(lh.elementType()).isEqualTo(CompactType.I32);
    }

    @Test
    void readListHeader_largeList() throws Exception {
        // size>=15 marker (0xf5), then varint size 100
        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(new byte[] {(byte) 0xf5, 0x64}));
        CompactProtocolReader.ListHeader lh = r.readListHeader();
        assertThat(lh.size()).isEqualTo(100);
        assertThat(lh.elementType()).isEqualTo(CompactType.I32);
    }
}
