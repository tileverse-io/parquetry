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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.EncodingStats;
import io.tileverse.parquetry.format.KeyValue;
import io.tileverse.parquetry.format.SizeStatistics;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.format.enums.CompressionCodec;
import io.tileverse.parquetry.format.enums.Encoding;
import io.tileverse.parquetry.format.enums.Type;

/**
 * Deserializer for the Thrift {@code ColumnMetaData} struct.
 *
 * <pre>
 * struct ColumnMetaData {
 *   1: required Type type
 *   2: required list&lt;Encoding&gt; encodings
 *   3: required list&lt;string&gt; path_in_schema
 *   4: required CompressionCodec codec
 *   5: required i64 num_values
 *   6: required i64 total_uncompressed_size
 *   7: required i64 total_compressed_size
 *   8: optional list&lt;KeyValue&gt; key_value_metadata
 *   9: required i64 data_page_offset
 *   10: optional i64 index_page_offset
 *   11: optional i64 dictionary_page_offset
 *   12: optional Statistics statistics
 *   13: optional list&lt;PageEncodingStats&gt; encoding_stats
 *   14: optional i64 bloom_filter_offset
 *   15: optional i32 bloom_filter_length
 *   16: optional SizeStatistics size_statistics
 * }
 * </pre>
 */
final class ColumnMetaDataDeserializer {

    private ColumnMetaDataDeserializer() {}

    static ColumnMetaData read(CompactProtocolReader r) throws IOException {
        Type type = Type.BOOLEAN;
        List<Encoding> encodings = new ArrayList<>();
        List<String> pathInSchema = new ArrayList<>();
        CompressionCodec codec = CompressionCodec.UNCOMPRESSED;
        long numValues = 0;
        long totalUncompressedSize = 0;
        long totalCompressedSize = 0;
        List<KeyValue> keyValueMetadata = new ArrayList<>();
        long dataPageOffset = 0;
        OptionalLong indexPageOffset = OptionalLong.empty();
        OptionalLong dictionaryPageOffset = OptionalLong.empty();
        Optional<Statistics> statistics = Optional.empty();
        List<EncodingStats> encodingStats = new ArrayList<>();
        OptionalLong bloomFilterOffset = OptionalLong.empty();
        OptionalLong bloomFilterLength = OptionalLong.empty();
        Optional<SizeStatistics> sizeStatistics = Optional.empty();
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) {
                break;
            }
            lastFieldId = fh.fieldId();
            switch (fh.fieldId()) {
                case 1 -> type = Type.values()[r.readI32()];
                case 2 -> encodings = readEncodingList(r);
                case 3 -> pathInSchema = readStringList(r);
                case 4 -> codec = CompressionCodec.values()[r.readI32()];
                case 5 -> numValues = r.readI64();
                case 6 -> totalUncompressedSize = r.readI64();
                case 7 -> totalCompressedSize = r.readI64();
                case 8 -> keyValueMetadata = readKeyValueList(r);
                case 9 -> dataPageOffset = r.readI64();
                case 10 -> indexPageOffset = OptionalLong.of(r.readI64());
                case 11 -> dictionaryPageOffset = OptionalLong.of(r.readI64());
                case 12 -> statistics = Optional.of(StatisticsDeserializer.read(r));
                case 13 -> encodingStats = readEncodingStatsList(r);
                case 14 -> bloomFilterOffset = OptionalLong.of(r.readI64());
                case 15 -> bloomFilterLength = OptionalLong.of(r.readI32());
                case 16 -> sizeStatistics = Optional.of(SizeStatisticsDeserializer.read(r));
                default -> r.skipField(fh.type());
            }
        }
        return new ColumnMetaData(
                type,
                encodings,
                pathInSchema,
                codec,
                numValues,
                totalUncompressedSize,
                totalCompressedSize,
                keyValueMetadata,
                dataPageOffset,
                indexPageOffset,
                dictionaryPageOffset,
                statistics,
                encodingStats,
                bloomFilterOffset,
                bloomFilterLength,
                sizeStatistics);
    }

    private static List<Encoding> readEncodingList(CompactProtocolReader r) throws IOException {
        CompactProtocolReader.ListHeader lh = r.readListHeader();
        List<Encoding> result = new ArrayList<>(lh.size());
        for (int i = 0; i < lh.size(); i++) {
            result.add(Encoding.values()[r.readI32()]);
        }
        return result;
    }

    private static List<String> readStringList(CompactProtocolReader r) throws IOException {
        CompactProtocolReader.ListHeader lh = r.readListHeader();
        List<String> result = new ArrayList<>(lh.size());
        for (int i = 0; i < lh.size(); i++) {
            result.add(r.readString());
        }
        return result;
    }

    private static List<KeyValue> readKeyValueList(CompactProtocolReader r) throws IOException {
        CompactProtocolReader.ListHeader lh = r.readListHeader();
        List<KeyValue> result = new ArrayList<>(lh.size());
        for (int i = 0; i < lh.size(); i++) {
            result.add(KeyValueDeserializer.read(r));
        }
        return result;
    }

    private static List<EncodingStats> readEncodingStatsList(CompactProtocolReader r) throws IOException {
        CompactProtocolReader.ListHeader lh = r.readListHeader();
        List<EncodingStats> result = new ArrayList<>(lh.size());
        for (int i = 0; i < lh.size(); i++) {
            result.add(EncodingStatsDeserializer.read(r));
        }
        return result;
    }
}
