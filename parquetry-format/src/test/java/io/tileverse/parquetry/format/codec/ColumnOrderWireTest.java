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
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.ColumnOrder;

/**
 * Wire-level coverage for the {@code ColumnOrder} union: the three cases defined by the format read back as themselves,
 * and a case added by a newer writer reads as {@link ColumnOrder.Unknown} without derailing the bytes that follow.
 */
class ColumnOrderWireTest {

    private static final int SENTINEL = 0x5A;

    static Stream<ColumnOrder> orders() {
        return Stream.of(
                new ColumnOrder.TypeDefined(),
                new ColumnOrder.Ieee754TotalOrder(),
                new ColumnOrder.Int96TimestampOrder(),
                new ColumnOrder.Unknown((short) 4),
                new ColumnOrder.Unknown((short) 300),
                new ColumnOrder.Unknown(ColumnOrder.Unknown.NO_CASE));
    }

    @ParameterizedTest
    @MethodSource("orders")
    void orderRoundTripsAcrossWriterAndReader(ColumnOrder original) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ColumnOrderSerializer.serialize(new CompactProtocolWriter(bytes), original);

        Decoded decoded = read(bytes.toByteArray());

        assertThat(decoded.order()).isEqualTo(original);
        assertThat(decoded.nextByte()).as("byte following the union").isEqualTo(SENTINEL);
    }

    @Test
    void emptyUnionReadsAsAnOrderNamingNoCase() throws IOException {
        byte[] emptyUnion = {0x00};

        Decoded decoded = read(emptyUnion);

        assertThat(decoded.order()).isEqualTo(new ColumnOrder.Unknown(ColumnOrder.Unknown.NO_CASE));
        assertThat(decoded.nextByte()).as("byte following the union").isEqualTo(SENTINEL);
    }

    @Test
    void orderNamingNoCaseIsWrittenAsAnEmptyUnion() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        ColumnOrderSerializer.serialize(
                new CompactProtocolWriter(bytes), new ColumnOrder.Unknown(ColumnOrder.Unknown.NO_CASE));

        assertThat(bytes.toByteArray()).containsExactly(0x00);
    }

    @Test
    void unknownCaseWithAPayloadIsSkippedUpToTheEndOfTheUnion() throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CompactProtocolWriter writer = new CompactProtocolWriter(bytes);
        writer.writeStructBegin();
        writer.writeFieldBegin((short) 7, CompactType.STRUCT);
        writer.writeStructBegin();
        writer.writeI32Field((short) 1, 123);
        writer.writeFieldStop();
        writer.writeStructEnd();
        writer.writeFieldStop();
        writer.writeStructEnd();

        Decoded decoded = read(bytes.toByteArray());

        assertThat(decoded.order()).isEqualTo(new ColumnOrder.Unknown((short) 7));
        assertThat(decoded.nextByte()).as("byte following the union").isEqualTo(SENTINEL);
    }

    /** Reads one union from {@code wire} followed by a sentinel byte, and reports the byte left unread after it. */
    private static Decoded read(byte[] wire) throws IOException {
        byte[] withSentinel = new byte[wire.length + 1];
        System.arraycopy(wire, 0, withSentinel, 0, wire.length);
        withSentinel[wire.length] = (byte) SENTINEL;
        CompactProtocolReader reader = new CompactProtocolReader(new ByteArrayInputStream(withSentinel));
        ColumnOrder order = ColumnOrderDeserializer.read(reader);
        return new Decoded(order, reader.readByte());
    }

    /** The order read from the wire and the first byte left unread after it. */
    private record Decoded(ColumnOrder order, int nextByte) {}
}
