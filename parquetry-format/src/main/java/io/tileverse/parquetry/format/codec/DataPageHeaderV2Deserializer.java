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

import java.io.IOException;
import java.util.Optional;

import io.tileverse.parquetry.format.DataPageHeaderV2;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.Statistics;

/**
 * Deserializer for the Thrift {@code DataPageHeaderV2} struct.
 *
 * <pre>
 * struct DataPageHeaderV2 {
 *   1: required i32 num_values
 *   2: required i32 num_nulls
 *   3: required i32 num_rows
 *   4: required Encoding encoding
 *   5: required i32 definition_levels_byte_length
 *   6: required i32 repetition_levels_byte_length
 *   7: optional bool is_compressed
 *   8: optional Statistics statistics
 * }
 * </pre>
 */
final class DataPageHeaderV2Deserializer {

    private DataPageHeaderV2Deserializer() {}

    static DataPageHeaderV2 read(CompactProtocolReader r) throws IOException {
        int numValues = 0;
        int numNulls = 0;
        int numRows = 0;
        Encoding encoding = Encoding.PLAIN;
        int definitionLevelsByteLength = 0;
        int repetitionLevelsByteLength = 0;
        // Thrift schema says is_compressed defaults to true; the absent / present-but-true / present-but-false cases
        // collapse into a single boolean because the consumer only needs to know whether to decompress.
        boolean isCompressed = true;
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
                case 2 -> numNulls = r.readI32();
                case 3 -> numRows = r.readI32();
                case 4 -> encoding = Encoding.valueOf(r.readI32());
                case 5 -> definitionLevelsByteLength = r.readI32();
                case 6 -> repetitionLevelsByteLength = r.readI32();
                case 7 -> isCompressed = fh.type() == CompactType.BOOLEAN_TRUE;
                case 8 -> statistics = Optional.of(StatisticsDeserializer.read(r));
                default -> r.skipField(fh.type());
            }
        }
        return new DataPageHeaderV2(
                numValues,
                numNulls,
                numRows,
                encoding,
                definitionLevelsByteLength,
                repetitionLevelsByteLength,
                isCompressed,
                statistics);
    }
}
