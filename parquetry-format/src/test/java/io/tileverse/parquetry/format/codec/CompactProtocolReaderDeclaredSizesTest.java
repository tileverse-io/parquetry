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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.MalformedFileException;

/**
 * A length or an element count inside a Thrift structure comes from untrusted bytes. One beyond the bytes of the stream
 * fails at the end of the stream, and no memory is allocated for the declared size.
 */
class CompactProtocolReaderDeclaredSizesTest {

    /** The varint of {@code 2^31 - 1}. */
    private static final byte[] TWO_GB = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x07};

    /** The varint of {@code 2^32 + 3}, with low 32 bits reading 3. */
    private static final byte[] BEYOND_THE_INT_RANGE = {(byte) 0x83, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x10};

    private static final byte[] THREE_BYTES = {1, 2, 1};

    /** The header of a list of booleans declaring its size in the varint after it. */
    private static final byte[] LONG_LIST_OF_BOOLEANS = {(byte) 0xF1};

    /** The same header for a list of 32-bit integers. */
    private static final byte[] LONG_LIST_OF_INTEGERS = {(byte) 0xF5};

    @Test
    void binaryDeclaredLongerThanTheStreamFailsAtTheEndOfTheStream() {
        CompactProtocolReader reader = readerOver(TWO_GB, THREE_BYTES);

        assertThatThrownBy(reader::readBinary).isInstanceOf(EOFException.class);
    }

    @Test
    void stringDeclaredLongerThanTheStreamFailsAtTheEndOfTheStream() {
        CompactProtocolReader reader = readerOver(TWO_GB, THREE_BYTES);

        assertThatThrownBy(reader::readString).isInstanceOf(EOFException.class);
    }

    @Test
    void lengthBeyondTheIntRangeIsMalformed() {
        CompactProtocolReader reader = readerOver(BEYOND_THE_INT_RANGE, THREE_BYTES);

        assertThatThrownBy(reader::readBinary).isInstanceOf(MalformedFileException.class);
    }

    @Test
    void listSizeBeyondTheIntRangeIsMalformed() {
        CompactProtocolReader reader = readerOver(LONG_LIST_OF_INTEGERS, BEYOND_THE_INT_RANGE);

        assertThatThrownBy(reader::readListHeader).isInstanceOf(MalformedFileException.class);
    }

    @Test
    void listIsSizedUpFrontForABoundedCountOfElements() throws IOException {
        CompactProtocolReader reader = readerOver(LONG_LIST_OF_INTEGERS, TWO_GB);

        CompactProtocolReader.ListHeader declared = reader.readListHeader();

        assertThat(declared.size()).isEqualTo(Integer.MAX_VALUE);
        assertThat(declared.initialCapacity()).isEqualTo(1024);
    }

    @Test
    void smallListIsSizedUpFrontForItsElements() throws IOException {
        byte[] listOfThreeIntegers = {0x35};
        CompactProtocolReader reader = readerOver(listOfThreeIntegers);

        CompactProtocolReader.ListHeader declared = reader.readListHeader();

        assertThat(declared.initialCapacity()).isEqualTo(3);
    }

    @Test
    void skippedBinaryDeclaredLongerThanTheStreamFailsAtTheEndOfTheStream() {
        CompactProtocolReader reader = readerOver(TWO_GB, THREE_BYTES);

        assertThatThrownBy(() -> reader.skipField(CompactType.BINARY)).isInstanceOf(EOFException.class);
    }

    @Test
    void skippedListOfBooleansTakesOneByteAnElement() throws IOException {
        byte[] listOfThreeBooleans = {0x31};
        byte[] integerFieldNumberOne = {0x15};
        CompactProtocolReader reader = readerOver(listOfThreeBooleans, THREE_BYTES, integerFieldNumberOne);

        reader.skipField(CompactType.LIST);
        FieldHeader next = reader.readFieldHeader(0);

        assertThat(next.type()).isEqualTo(CompactType.I32);
        assertThat(next.fieldId()).isEqualTo((short) 1);
    }

    @Test
    void skippedListDeclaredLongerThanTheStreamFailsAtTheEndOfTheStream() {
        CompactProtocolReader reader = readerOver(LONG_LIST_OF_BOOLEANS, TWO_GB, THREE_BYTES);

        assertThatThrownBy(() -> reader.skipField(CompactType.LIST)).isInstanceOf(EOFException.class);
    }

    private static CompactProtocolReader readerOver(byte[]... parts) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            bytes.writeBytes(part);
        }
        return new CompactProtocolReader(new ByteArrayInputStream(bytes.toByteArray()));
    }
}
