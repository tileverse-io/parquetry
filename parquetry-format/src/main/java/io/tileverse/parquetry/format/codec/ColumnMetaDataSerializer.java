/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.GeospatialStatistics;
import io.tileverse.parquetry.format.SizeStatistics;
import io.tileverse.parquetry.format.Statistics;

/**
 * Serializer mirror of {@link ColumnMetaDataDeserializer}.
 *
 * <p>The reader treats both {@code key_value_metadata} (field 8) and {@code encoding_stats} (field 13) as plain lists
 * without an Optional wrapper, so this serializer omits them when the list is empty - matching what an existing reader
 * would have produced for absent fields.
 */
final class ColumnMetaDataSerializer {

    private ColumnMetaDataSerializer() {}

    static void serialize(CompactProtocolWriter w, ColumnMetaData cm) throws IOException {
        w.writeStructBegin();
        w.writeI32Field((short) 1, cm.type().value());
        w.writeListField((short) 2, CompactType.I32, cm.encodings(), ColumnMetaDataSerializer::writeEncoding);
        w.writeListField((short) 3, CompactType.BINARY, cm.pathInSchema(), CompactProtocolWriter::writeString);
        w.writeI32Field((short) 4, cm.codec().value());
        w.writeI64Field((short) 5, cm.numValues());
        w.writeI64Field((short) 6, cm.totalUncompressedSize());
        w.writeI64Field((short) 7, cm.totalCompressedSize());
        if (!cm.keyValueMetadata().isEmpty()) {
            w.writeListField((short) 8, CompactType.STRUCT, cm.keyValueMetadata(), KeyValueSerializer::serialize);
        }
        w.writeI64Field((short) 9, cm.dataPageOffset());
        if (cm.indexPageOffset().isPresent()) {
            w.writeI64Field((short) 10, cm.indexPageOffset().getAsLong());
        }
        if (cm.dictionaryPageOffset().isPresent()) {
            w.writeI64Field((short) 11, cm.dictionaryPageOffset().getAsLong());
        }
        Optional<Statistics> statistics = cm.statistics();
        if (statistics.isPresent()) {
            w.writeFieldBegin((short) 12, CompactType.STRUCT);
            StatisticsSerializer.serialize(w, statistics.get());
        }
        if (!cm.encodingStats().isEmpty()) {
            w.writeListField((short) 13, CompactType.STRUCT, cm.encodingStats(), EncodingStatsSerializer::serialize);
        }
        if (cm.bloomFilterOffset().isPresent()) {
            w.writeI64Field((short) 14, cm.bloomFilterOffset().getAsLong());
        }
        if (cm.bloomFilterLength().isPresent()) {
            // The deserializer reads field 15 as i32 (OptionalLong.of(r.readI32())); match that wire form here.
            w.writeI32Field((short) 15, (int) cm.bloomFilterLength().getAsLong());
        }
        Optional<SizeStatistics> sizeStatistics = cm.sizeStatistics();
        if (sizeStatistics.isPresent()) {
            w.writeFieldBegin((short) 16, CompactType.STRUCT);
            SizeStatisticsSerializer.serialize(w, sizeStatistics.get());
        }
        Optional<GeospatialStatistics> geospatialStatistics = cm.geospatialStatistics();
        if (geospatialStatistics.isPresent()) {
            w.writeFieldBegin((short) 17, CompactType.STRUCT);
            GeospatialStatisticsSerializer.serialize(w, geospatialStatistics.get());
        }
        w.writeFieldStop();
        w.writeStructEnd();
    }

    private static void writeEncoding(CompactProtocolWriter w, Encoding e) throws IOException {
        w.writeI32(e.value());
    }
}
