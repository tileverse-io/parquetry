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

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.LogicalType;

/**
 * Wire-level coverage for logical type annotations unknown to this reader, such as the FILE annotation (union field 19)
 * of a later version of the format. Such an annotation must not read as UNKNOWN (union field 11): UNKNOWN marks a
 * column holding only nulls, and a copy would write the column back as one.
 */
class LogicalTypeUnrecognizedWireTest {

    private static final int TYPE_STRUCT = 12;
    private static final int FILE_CASE = 19;

    @Test
    void laterCaseIsUnrecognized() throws IOException {
        LogicalType read = read(unionWithEmptyCase(FILE_CASE));

        assertThat(read).isEqualTo(new LogicalType.Unrecognized(FILE_CASE));
    }

    @Test
    void emptyUnionIsUnrecognized() throws IOException {
        LogicalType read = read(new byte[] {0});

        assertThat(read).isEqualTo(new LogicalType.Unrecognized(0));
    }

    @Test
    void unrecognizedIsNotWritable() {
        CompactProtocolWriter writer = new CompactProtocolWriter(new ByteArrayOutputStream());
        LogicalType unrecognized = new LogicalType.Unrecognized(FILE_CASE);

        assertThatThrownBy(() -> LogicalTypeSerializer.serialize(writer, unrecognized))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("union field 19");
    }

    private static LogicalType read(byte[] union) throws IOException {
        return LogicalTypeDeserializer.read(new CompactProtocolReader(new ByteArrayInputStream(union)));
    }

    /**
     * A union naming case {@code fieldId} with an empty struct body. Field ids above 15 do not fit the 4-bit delta of a
     * field header: the long form writes the type alone, then the field id as a zigzag varint.
     */
    private static byte[] unionWithEmptyCase(int fieldId) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(TYPE_STRUCT);
        out.write(fieldId << 1);
        out.write(0); // STOP of the case struct
        out.write(0); // STOP of the union
        return out.toByteArray();
    }
}
