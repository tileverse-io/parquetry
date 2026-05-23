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

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.format.ColumnIndex;

/** Serializer mirror of {@link ColumnIndexDeserializer}. */
final class ColumnIndexSerializer {

    private ColumnIndexSerializer() {}

    static void serialize(CompactProtocolWriter w, ColumnIndex ci) throws IOException {
        w.writeStructBegin();
        // Boolean list elements: BOOLEAN_TRUE / BOOLEAN_FALSE in the list element-type nibble would be ambiguous,
        // so the protocol uses the BYTE-encoded standalone form (0x01 / 0x00) via writeBool.
        w.writeListField((short) 1, CompactType.BOOLEAN_TRUE, ci.nullPages(), CompactProtocolWriter::writeBool);
        w.writeListField(
                (short) 2,
                CompactType.BINARY,
                ci.minValues(),
                (writer, seg) -> writer.writeBinary(seg.toArray(JAVA_BYTE)));
        w.writeListField(
                (short) 3,
                CompactType.BINARY,
                ci.maxValues(),
                (writer, seg) -> writer.writeBinary(seg.toArray(JAVA_BYTE)));
        w.writeI32Field((short) 4, ci.boundaryOrder().value());
        Optional<List<Long>> nullCounts = ci.nullCounts();
        if (nullCounts.isPresent()) {
            w.writeListField((short) 5, CompactType.I64, nullCounts.get(), CompactProtocolWriter::writeI64);
        }
        Optional<List<Long>> repetitionLevelHistograms = ci.repetitionLevelHistograms();
        if (repetitionLevelHistograms.isPresent()) {
            w.writeListField(
                    (short) 6, CompactType.I64, repetitionLevelHistograms.get(), CompactProtocolWriter::writeI64);
        }
        Optional<List<Long>> definitionLevelHistograms = ci.definitionLevelHistograms();
        if (definitionLevelHistograms.isPresent()) {
            w.writeListField(
                    (short) 7, CompactType.I64, definitionLevelHistograms.get(), CompactProtocolWriter::writeI64);
        }
        w.writeFieldStop();
        w.writeStructEnd();
    }
}
