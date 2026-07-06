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
package io.tileverse.parquetry.format;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Optional per-column-chunk size statistics; mirror of {@code SizeStatistics} in {@code parquet.thrift}.
 *
 * <p>Carried by {@link ColumnMetaData#sizeStatistics()}. {@link #unencodedByteArrayDataBytes()} reports the column's
 * raw {@code BYTE_ARRAY} payload size before encoding/compression; the two histograms count values per
 * repetition/definition level for nested-column cost estimation.
 *
 * @param unencodedByteArrayDataBytes total unencoded byte count for {@code BYTE_ARRAY} payload data, excluding the
 *     four-byte length prefixes; empty when the column is not {@code BYTE_ARRAY} or the writer omitted this
 * @param repetitionLevelHistogram count of values observed at each repetition level, indexed by level; empty when not
 *     recorded ({@code repetition_level_histogram} in the thrift schema)
 * @param definitionLevelHistogram count of values observed at each definition level, indexed by level; empty when not
 *     recorded ({@code definition_level_histogram} in the thrift schema)
 */
// S2789: constructor null-tolerates Optional to support the @Builder pattern.
@SuppressWarnings("java:S2789")
public record SizeStatistics(
        OptionalLong unencodedByteArrayDataBytes,
        Optional<List<Long>> repetitionLevelHistogram,
        Optional<List<Long>> definitionLevelHistogram) {

    public SizeStatistics {
        unencodedByteArrayDataBytes =
                unencodedByteArrayDataBytes == null ? OptionalLong.empty() : unencodedByteArrayDataBytes;
        repetitionLevelHistogram =
                repetitionLevelHistogram == null ? Optional.empty() : repetitionLevelHistogram.map(List::copyOf);
        definitionLevelHistogram =
                definitionLevelHistogram == null ? Optional.empty() : definitionLevelHistogram.map(List::copyOf);
    }
}
