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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

import io.tileverse.parquetry.format.Statistics;

/**
 * Serializer mirror of {@link StatisticsDeserializer}. The four binary fields ({@code max}, {@code min},
 * {@code max_value}, {@code min_value}) are absent on the wire when the segment is {@link MemorySegment#NULL}, matching
 * the deserializer's absent-segment sentinel.
 */
final class StatisticsSerializer {

    private StatisticsSerializer() {}

    static void serialize(CompactProtocolWriter w, Statistics s) throws IOException {
        w.writeStructBegin();
        writeBinaryIfPresent(w, (short) 1, s.max());
        writeBinaryIfPresent(w, (short) 2, s.min());
        if (s.nullCount().isPresent()) {
            w.writeI64Field((short) 3, s.nullCount().getAsLong());
        }
        if (s.distinctCount().isPresent()) {
            w.writeI64Field((short) 4, s.distinctCount().getAsLong());
        }
        writeBinaryIfPresent(w, (short) 5, s.maxValue());
        writeBinaryIfPresent(w, (short) 6, s.minValue());
        // is_max_value_exact and is_min_value_exact: only emit when true, since the deserializer treats absent and
        // false identically (the conservative default).
        if (s.isMaxValueExact()) {
            w.writeBoolField((short) 7, true);
        }
        if (s.isMinValueExact()) {
            w.writeBoolField((short) 8, true);
        }
        w.writeFieldStop();
        w.writeStructEnd();
    }

    private static void writeBinaryIfPresent(CompactProtocolWriter w, short fieldId, MemorySegment segment)
            throws IOException {
        if (segment == MemorySegment.NULL) {
            return;
        }
        byte[] bytes = segment.toArray(JAVA_BYTE);
        w.writeBinaryField(fieldId, bytes);
    }
}
