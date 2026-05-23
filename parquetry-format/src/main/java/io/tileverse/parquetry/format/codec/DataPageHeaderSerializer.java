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

/** Serializer mirror of {@link DataPageHeaderDeserializer}. */
final class DataPageHeaderSerializer {

    private DataPageHeaderSerializer() {}

    static void serialize(CompactProtocolWriter w, DataPageHeader h) throws IOException {
        w.writeStructBegin();
        w.writeI32Field((short) 1, h.numValues());
        w.writeI32Field((short) 2, h.encoding().value());
        w.writeI32Field((short) 3, h.definitionLevelEncoding().value());
        w.writeI32Field((short) 4, h.repetitionLevelEncoding().value());
        Optional<Statistics> statistics = h.statistics();
        if (statistics.isPresent()) {
            w.writeFieldBegin((short) 5, CompactType.STRUCT);
            StatisticsSerializer.serialize(w, statistics.get());
        }
        w.writeFieldStop();
        w.writeStructEnd();
    }
}
