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

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.format.SizeStatistics;

/** Serializer mirror of {@link SizeStatisticsDeserializer}. */
final class SizeStatisticsSerializer {

    private SizeStatisticsSerializer() {}

    static void serialize(CompactProtocolWriter w, SizeStatistics ss) throws IOException {
        w.writeStructBegin();
        if (ss.unencodedByteArrayDataBytes().isPresent()) {
            w.writeI64Field((short) 1, ss.unencodedByteArrayDataBytes().getAsLong());
        }
        Optional<List<Long>> repetitionLevelHistogram = ss.repetitionLevelHistogram();
        if (repetitionLevelHistogram.isPresent()) {
            w.writeListField(
                    (short) 2, CompactType.I64, repetitionLevelHistogram.get(), CompactProtocolWriter::writeI64);
        }
        Optional<List<Long>> definitionLevelHistogram = ss.definitionLevelHistogram();
        if (definitionLevelHistogram.isPresent()) {
            w.writeListField(
                    (short) 3, CompactType.I64, definitionLevelHistogram.get(), CompactProtocolWriter::writeI64);
        }
        w.writeFieldStop();
        w.writeStructEnd();
    }
}
