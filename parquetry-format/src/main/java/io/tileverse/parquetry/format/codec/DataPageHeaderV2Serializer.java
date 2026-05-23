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

import io.tileverse.parquetry.format.DataPageHeaderV2;
import io.tileverse.parquetry.format.Statistics;

/**
 * Serializer mirror of {@link DataPageHeaderV2Deserializer}.
 *
 * <p>{@code is_compressed} is always emitted: the Thrift schema defaults to true when absent, and our record models the
 * field as a plain boolean. Emitting the value explicitly keeps the wire bytes self-describing and avoids relying on
 * the reader's default-when-absent.
 */
final class DataPageHeaderV2Serializer {

    private DataPageHeaderV2Serializer() {}

    static void serialize(CompactProtocolWriter w, DataPageHeaderV2 h) throws IOException {
        w.writeStructBegin();
        w.writeI32Field((short) 1, h.numValues());
        w.writeI32Field((short) 2, h.numNulls());
        w.writeI32Field((short) 3, h.numRows());
        w.writeI32Field((short) 4, h.encoding().value());
        w.writeI32Field((short) 5, h.definitionLevelsByteLength());
        w.writeI32Field((short) 6, h.repetitionLevelsByteLength());
        w.writeBoolField((short) 7, h.isCompressed());
        Optional<Statistics> statistics = h.statistics();
        if (statistics.isPresent()) {
            w.writeFieldBegin((short) 8, CompactType.STRUCT);
            StatisticsSerializer.serialize(w, statistics.get());
        }
        w.writeFieldStop();
        w.writeStructEnd();
    }
}
