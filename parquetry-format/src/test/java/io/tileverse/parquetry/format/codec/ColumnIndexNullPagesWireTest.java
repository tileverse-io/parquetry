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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.BoundaryOrder;
import io.tileverse.parquetry.format.ColumnIndex;

/**
 * Wire-level coverage for {@code ColumnIndex.null_pages} (field 1), a list of bool. The Thrift Compact Protocol sends
 * each element as one byte, 1 for true and 2 for false, and strict readers (the Rust thrift crate, for one) reject a
 * column index holding any other byte.
 */
class ColumnIndexNullPagesWireTest {

    private static final int NULL_PAGES_FIELD_HEADER = 0x19;
    private static final int LIST_OF_TWO_BOOLS_HEADER = 0x21;
    private static final int TRUE_ELEMENT = 0x01;
    private static final int FALSE_ELEMENT = 0x02;

    @Test
    void nullPagesAreWrittenAsOneForTrueAndTwoForFalse() throws IOException {
        byte[] wire = write(columnIndex(true, false));

        assertThat(wire).startsWith(NULL_PAGES_FIELD_HEADER, LIST_OF_TWO_BOOLS_HEADER, TRUE_ELEMENT, FALSE_ELEMENT);
    }

    @Test
    void nullPagesRoundTrip() throws IOException {
        ColumnIndex decoded = read(write(columnIndex(true, false)));

        assertThat(decoded.nullPages()).containsExactly(true, false);
    }

    @Test
    void nullPagesWrittenWithZeroForFalseReadBack() throws IOException {
        byte[] wire = write(columnIndex(true, false));
        int falseElementOffset = 3;
        wire[falseElementOffset] = 0x00;

        ColumnIndex decoded = read(wire);

        assertThat(decoded.nullPages()).containsExactly(true, false);
    }

    private static ColumnIndex columnIndex(boolean firstPageIsNull, boolean secondPageIsNull) {
        MemorySegment bound = MemorySegment.ofArray(new byte[] {7, 0, 0, 0});
        return new ColumnIndex(
                List.of(firstPageIsNull, secondPageIsNull),
                List.of(bound, bound),
                List.of(bound, bound),
                BoundaryOrder.UNORDERED,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static byte[] write(ColumnIndex index) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ColumnIndexSerializer.serialize(new CompactProtocolWriter(bytes), index);
        return bytes.toByteArray();
    }

    private static ColumnIndex read(byte[] wire) throws IOException {
        return ColumnIndexDeserializer.read(new CompactProtocolReader(new ByteArrayInputStream(wire)));
    }
}
