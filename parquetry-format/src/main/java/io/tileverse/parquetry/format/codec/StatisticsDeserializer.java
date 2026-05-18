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

import io.tileverse.parquetry.format.Statistics;

/**
 * Deserializer for the Thrift {@code Statistics} struct.
 *
 * <pre>
 * struct Statistics {
 *   1: optional binary max
 *   2: optional binary min
 *   3: optional i64 null_count
 *   4: optional i64 distinct_count
 *   5: optional binary max_value
 *   6: optional binary min_value
 *   7: optional bool is_max_value_exact
 *   8: optional bool is_min_value_exact
 * }
 * </pre>
 */
final class StatisticsDeserializer {

    private StatisticsDeserializer() {}

    static Statistics read(CompactProtocolReader r) throws IOException {
        Optional<byte[]> max = Optional.empty();
        Optional<byte[]> min = Optional.empty();
        Optional<Long> nullCount = Optional.empty();
        Optional<Long> distinctCount = Optional.empty();
        Optional<byte[]> maxValue = Optional.empty();
        Optional<byte[]> minValue = Optional.empty();
        Optional<Boolean> isMaxValueExact = Optional.empty();
        Optional<Boolean> isMinValueExact = Optional.empty();
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) {
                break;
            }
            lastFieldId = fh.fieldId();
            switch (fh.fieldId()) {
                case 1 -> max = Optional.of(r.readBinary());
                case 2 -> min = Optional.of(r.readBinary());
                case 3 -> nullCount = Optional.of(r.readI64());
                case 4 -> distinctCount = Optional.of(r.readI64());
                case 5 -> maxValue = Optional.of(r.readBinary());
                case 6 -> minValue = Optional.of(r.readBinary());
                case 7 -> isMaxValueExact = Optional.of(fh.type() == CompactType.BOOLEAN_TRUE);
                case 8 -> isMinValueExact = Optional.of(fh.type() == CompactType.BOOLEAN_TRUE);
                default -> r.skipField(fh.type());
            }
        }
        return new Statistics(max, min, nullCount, distinctCount, maxValue, minValue, isMaxValueExact, isMinValueExact);
    }
}
