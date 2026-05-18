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

import io.tileverse.parquetry.format.enums.CompressionCodec;
import io.tileverse.parquetry.format.enums.Encoding;
import io.tileverse.parquetry.format.enums.Type;

public record ColumnMetaData(
        Type type,
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
        Optional<SizeStatistics> sizeStatistics) {

    public ColumnMetaData {
        encodings = List.copyOf(encodings);
        pathInSchema = List.copyOf(pathInSchema);
        keyValueMetadata = List.copyOf(keyValueMetadata);
        encodingStats = List.copyOf(encodingStats);
    }
}
