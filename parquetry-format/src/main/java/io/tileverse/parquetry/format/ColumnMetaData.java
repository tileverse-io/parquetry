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
package io.tileverse.parquetry.format;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Per-column-chunk metadata; mirror of {@code ColumnMetaData} in {@code parquet.thrift}.
 *
 * <p>Stored in {@link ColumnChunk#metaData()} when the chunk is not encrypted. Holds the column's {@link PhysicalType},
 * the {@link Encoding}s used across its pages, and the {@link CompressionCodec}, plus value and byte counts. The byte
 * offset of the first data page is required; dictionary, index, and bloom-filter offsets are present only when those
 * structures were written. {@link Statistics} are optional.
 *
 * @param type physical {@link PhysicalType} of this column's values
 * @param encodings set of every {@link Encoding} used by any page in this column chunk (data, dictionary, and levels)
 * @param pathInSchema dotted path from the schema root to this leaf column, one segment per nesting level
 * @param codec compression {@link CompressionCodec} applied to this chunk's pages
 * @param numValues total number of values in this column chunk, including nulls
 * @param totalUncompressedSize total uncompressed byte size of all pages in this column chunk, including page headers
 *     ({@code total_uncompressed_size} in the thrift schema)
 * @param totalCompressedSize total compressed byte size of all pages in this column chunk, including page headers
 *     ({@code total_compressed_size} in the thrift schema)
 * @param keyValueMetadata optional column-level key/value metadata; empty list when none was written
 * @param dataPageOffset byte offset of the first data page in the file ({@code data_page_offset} in the thrift schema)
 * @param indexPageOffset byte offset of the column's index page; unset when no index page was written
 *     ({@code index_page_offset} in the thrift schema)
 * @param dictionaryPageOffset byte offset of the column's dictionary page; unset when the chunk is not
 *     dictionary-encoded ({@code dictionary_page_offset} in the thrift schema)
 * @param statistics optional column-chunk-level {@link Statistics}; empty when the writer recorded none
 * @param encodingStats per-({@link PageType}, {@link Encoding}) page counts; empty list when not written
 * @param bloomFilterOffset byte offset of this column's bloom filter; unset when no bloom filter was written
 *     ({@code bloom_filter_offset} in the thrift schema)
 * @param bloomFilterLength byte length of this column's bloom filter including header; unset when no bloom filter was
 *     written ({@code bloom_filter_length} in the thrift schema)
 * @param sizeStatistics optional {@link SizeStatistics} for memory estimation and filter pushdown
 * @param geospatialStatistics optional {@link GeospatialStatistics} for GEOMETRY and GEOGRAPHY columns; carries
 *     bounding-box and observed-WKB-type metadata used by spatial pruning ({@code geospatial_statistics} in the thrift
 *     schema)
 */
public record ColumnMetaData(
        PhysicalType type,
        List<Encoding> encodings,
        List<String> pathInSchema,
        CompressionCodec codec,
        long numValues,
        long totalUncompressedSize,
        long totalCompressedSize,
        List<KeyValue> keyValueMetadata,
        long dataPageOffset,
        OptionalLong indexPageOffset,
        OptionalLong dictionaryPageOffset,
        Optional<Statistics> statistics,
        List<EncodingStats> encodingStats,
        OptionalLong bloomFilterOffset,
        OptionalLong bloomFilterLength,
        Optional<SizeStatistics> sizeStatistics,
        Optional<GeospatialStatistics> geospatialStatistics) {

    public ColumnMetaData {
        encodings = List.copyOf(encodings);
        pathInSchema = List.copyOf(pathInSchema);
        keyValueMetadata = List.copyOf(keyValueMetadata);
        encodingStats = List.copyOf(encodingStats);
    }
}
