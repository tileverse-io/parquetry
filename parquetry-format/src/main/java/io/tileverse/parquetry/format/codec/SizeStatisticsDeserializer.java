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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import io.tileverse.parquetry.format.SizeStatistics;

/**
 * Deserializer for the Thrift {@code SizeStatistics} struct.
 *
 * <pre>
 * struct SizeStatistics {
 *   1: optional i64 unencoded_byte_array_data_bytes
 *   2: optional list&lt;i64&gt; repetition_level_histogram
 *   3: optional list&lt;i64&gt; definition_level_histogram
 * }
 * </pre>
 */
final class SizeStatisticsDeserializer {

    private SizeStatisticsDeserializer() {}

    static SizeStatistics read(CompactProtocolReader r) throws IOException {
        OptionalLong unencodedByteArrayDataBytes = OptionalLong.empty();
        Optional<List<Long>> repetitionLevelHistogram = Optional.empty();
        Optional<List<Long>> definitionLevelHistogram = Optional.empty();
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) {
                break;
            }
            lastFieldId = fh.fieldId();
            switch (fh.fieldId()) {
                case 1 -> unencodedByteArrayDataBytes = OptionalLong.of(r.readI64());
                case 2 -> repetitionLevelHistogram = Optional.of(readI64List(r));
                case 3 -> definitionLevelHistogram = Optional.of(readI64List(r));
                default -> r.skipField(fh.type());
            }
        }
        return new SizeStatistics(unencodedByteArrayDataBytes, repetitionLevelHistogram, definitionLevelHistogram);
    }

    private static List<Long> readI64List(CompactProtocolReader r) throws IOException {
        CompactProtocolReader.ListHeader lh = r.readListHeader();
        List<Long> result = new ArrayList<>(lh.size());
        for (int i = 0; i < lh.size(); i++) {
            result.add(r.readI64());
        }
        return result;
    }
}
