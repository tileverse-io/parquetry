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

import java.io.IOException;
import java.util.Optional;

import io.tileverse.parquetry.format.DataPageHeader;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.format.enums.Encoding;

/**
 * Deserializer for the Thrift {@code DataPageHeader} struct.
 *
 * <pre>
 * struct DataPageHeader {
 *   1: required i32 num_values
 *   2: required Encoding encoding
 *   3: required Encoding definition_level_encoding
 *   4: required Encoding repetition_level_encoding
 *   5: optional Statistics statistics
 * }
 * </pre>
 */
final class DataPageHeaderDeserializer {

    private DataPageHeaderDeserializer() {}

    static DataPageHeader read(CompactProtocolReader r) throws IOException {
        int numValues = 0;
        Encoding encoding = Encoding.PLAIN;
        Encoding definitionLevelEncoding = Encoding.PLAIN;
        Encoding repetitionLevelEncoding = Encoding.PLAIN;
        Optional<Statistics> statistics = Optional.empty();
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) {
                break;
            }
            lastFieldId = fh.fieldId();
            switch (fh.fieldId()) {
                case 1 -> numValues = r.readI32();
                case 2 -> encoding = Encoding.values()[r.readI32()];
                case 3 -> definitionLevelEncoding = Encoding.values()[r.readI32()];
                case 4 -> repetitionLevelEncoding = Encoding.values()[r.readI32()];
                case 5 -> statistics = Optional.of(StatisticsDeserializer.read(r));
                default -> r.skipField(fh.type());
            }
        }
        return new DataPageHeader(numValues, encoding, definitionLevelEncoding, repetitionLevelEncoding, statistics);
    }
}
