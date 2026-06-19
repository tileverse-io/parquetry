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
package io.tileverse.parquetry.format.codec;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.tileverse.parquetry.format.LogicalType;

/**
 * Wire-level coverage for {@code IntType} (LogicalType union field 10). Its {@code bitWidth} is a Thrift {@code i8}:
 * one raw signed byte, not a zigzag-encoded {@code i32}. Reading or writing it as {@code i32} halves the value on the
 * wire (a raw {@code 0x08} byte zigzag-decodes to 4), which mis-classifies an 8-bit integer as a 4-bit one. These tests
 * pin the raw byte and the round-trip across the writer and reader.
 */
class LogicalTypeIntTypeWireTest {

    private static final int BIT_WIDTH_FIELD_HEADER = (1 << 4) | CompactType.BYTE.code;

    @ParameterizedTest
    @ValueSource(ints = {8, 16, 32, 64})
    void bitWidthRoundTripsAcrossWriterAndReader(int bitWidth) throws IOException {
        LogicalType.IntType original = new LogicalType.IntType((byte) bitWidth, true);

        LogicalType decoded = roundTrip(original);

        assertThat(decoded).isEqualTo(original);
    }

    @Test
    void bitWidthIsEncodedAsOneRawSignedByte() throws IOException {
        byte[] wire = serialize(new LogicalType.IntType((byte) 8, true));

        int fieldHeaderIndex = indexOf(wire, (byte) BIT_WIDTH_FIELD_HEADER);
        assertThat(fieldHeaderIndex).as("bitWidth field header present").isNotNegative();
        assertThat(wire[fieldHeaderIndex + 1])
                .as("bitWidth is the raw i8 value, not zigzag-encoded")
                .isEqualTo((byte) 8);
    }

    private static LogicalType roundTrip(LogicalType.IntType original) throws IOException {
        byte[] wire = serialize(original);
        CompactProtocolReader reader = new CompactProtocolReader(new ByteArrayInputStream(wire));
        return LogicalTypeDeserializer.read(reader);
    }

    private static byte[] serialize(LogicalType.IntType intType) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CompactProtocolWriter writer = new CompactProtocolWriter(bytes);
        LogicalTypeSerializer.serialize(writer, intType);
        return bytes.toByteArray();
    }

    private static int indexOf(byte[] bytes, byte target) {
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == target) {
                return i;
            }
        }
        return -1;
    }
}
