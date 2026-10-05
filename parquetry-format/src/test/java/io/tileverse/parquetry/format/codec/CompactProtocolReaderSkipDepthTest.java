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
import java.io.IOException;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.MalformedFileException;

/**
 * Skipping an unknown field descends into its lists and structs, to a depth read from untrusted bytes. Beyond a bound
 * the nesting is malformed; a descent without one exhausts the stack.
 */
class CompactProtocolReaderSkipDepthTest {

    /** The header of a list of one list: repeated, each byte nests one more list. */
    private static final byte LIST_OF_ONE_LIST = 0x19;

    /**
     * The header of a struct field numbered one above the field before it: repeated, each byte nests one more struct.
     */
    private static final byte NEXT_FIELD_IS_A_STRUCT = 0x1C;

    private static final int FAR_BEYOND_THE_BOUND = 1_000_000;

    @Test
    void skippedListsNestedBeyondTheBoundAreMalformed() {
        CompactProtocolReader reader = readerOver(repeated(LIST_OF_ONE_LIST, FAR_BEYOND_THE_BOUND));

        assertThatThrownBy(() -> reader.skipField(CompactType.LIST)).isInstanceOf(MalformedFileException.class);
    }

    @Test
    void skippedStructsNestedBeyondTheBoundAreMalformed() {
        CompactProtocolReader reader = readerOver(repeated(NEXT_FIELD_IS_A_STRUCT, FAR_BEYOND_THE_BOUND));

        assertThatThrownBy(() -> reader.skipField(CompactType.STRUCT)).isInstanceOf(MalformedFileException.class);
    }

    @Test
    void skippedListsNestedWithinTheBoundAreReadOver() throws IOException {
        byte[] nineNestedLists = repeated(LIST_OF_ONE_LIST, 9);
        byte[] listOfOneInteger = {0x15, 0x02};
        byte[] integerFieldNumberOne = {0x15};
        CompactProtocolReader reader = readerOver(nineNestedLists, listOfOneInteger, integerFieldNumberOne);

        reader.skipField(CompactType.LIST);
        FieldHeader next = reader.readFieldHeader(0);

        assertThat(next.type()).isEqualTo(CompactType.I32);
        assertThat(next.fieldId()).isEqualTo((short) 1);
    }

    @Test
    void listsSkippedOneAfterTheOtherDoNotAddUp() throws IOException {
        byte[] listOfOneInteger = {0x15, 0x02};
        CompactProtocolReader reader = readerOver(repeated(listOfOneInteger, 1000));

        for (int list = 0; list < 1000; list++) {
            reader.skipField(CompactType.LIST);
        }

        assertThat(reader.readByte()).as("end of the stream").isEqualTo(-1);
    }

    private static byte[] repeated(byte value, int times) {
        byte[] bytes = new byte[times];
        Arrays.fill(bytes, value);
        return bytes;
    }

    private static byte[] repeated(byte[] part, int times) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        for (int i = 0; i < times; i++) {
            bytes.writeBytes(part);
        }
        return bytes.toByteArray();
    }

    private static CompactProtocolReader readerOver(byte[]... parts) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            bytes.writeBytes(part);
        }
        return new CompactProtocolReader(new ByteArrayInputStream(bytes.toByteArray()));
    }
}
