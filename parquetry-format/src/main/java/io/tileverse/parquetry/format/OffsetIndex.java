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
package io.tileverse.parquetry.format;

import java.util.List;
import java.util.Optional;

/**
 * Per-page byte offsets for one column chunk; mirror of {@code OffsetIndex} in {@code parquet.thrift}.
 *
 * <p>One {@link PageLocation} per data page, parallel to {@link ColumnIndex}. A reader can seek straight to a given
 * page without walking page headers. Stored at {@link ColumnChunk#offsetIndexOffset()}.
 *
 * @param pageLocations {@link PageLocation} per data page, ordered by ascending {@link PageLocation#offset()}
 * @param unencodedByteArrayDataBytes per-page unencoded/uncompressed byte size for {@code BYTE_ARRAY} columns; empty
 *     when not recorded ({@code unencoded_byte_array_data_bytes} in the thrift schema)
 * @see ParquetFormat#readOffsetIndex(io.tileverse.parquetry.io.ByteRangeSource, long, int)
 */
public record OffsetIndex(List<PageLocation> pageLocations, Optional<List<Long>> unencodedByteArrayDataBytes) {

    public OffsetIndex {
        pageLocations = List.copyOf(pageLocations);
        unencodedByteArrayDataBytes = unencodedByteArrayDataBytes.map(List::copyOf);
    }
}
