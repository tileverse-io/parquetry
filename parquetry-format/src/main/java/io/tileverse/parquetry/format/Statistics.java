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

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.OptionalLong;

import lombok.Builder;

/**
 * Per-column statistics carried in {@link ColumnMetaData}.
 *
 * <p>The (deprecated) {@code max}/{@code min} byte arrays are the legacy fields written by older parquet writers;
 * modern writers use {@code maxValue}/{@code minValue} which obey the column's {@link ColumnOrder}. Both pairs are
 * optional.
 *
 * <p>All {@link ByteBuffer} fields are read-only; callers cannot mutate the backing bytes.
 *
 * @param max deprecated legacy max bytes; PLAIN-encoded per the column's physical type. Empty when the writer used the
 *     newer {@link #maxValue} pair or recorded no max
 * @param min deprecated legacy min bytes; PLAIN-encoded. Empty when the writer used {@link #minValue} or recorded no
 *     min
 * @param nullCount number of null values across the row group / page covered by these stats; empty when not recorded
 * @param distinctCount approximate count of distinct non-null values; empty when not recorded
 * @param maxValue PLAIN-encoded max bytes obeying the column's {@link ColumnOrder}; the modern field, empty when the
 *     writer only recorded the legacy {@link #max}
 * @param minValue PLAIN-encoded min bytes obeying the column's {@link ColumnOrder}; the modern field, empty when the
 *     writer only recorded the legacy {@link #min}
 * @param isMaxValueExact {@code true} when {@link #maxValue} is the actual maximum; {@code false} (the conservative
 *     default for absent / explicit-false) means the writer may have rounded up
 * @param isMinValueExact {@code true} when {@link #minValue} is the actual minimum; {@code false} means the writer may
 *     have rounded down
 */
// S2789: constructor null-tolerates Optional to support the @Builder pattern.
@SuppressWarnings("java:S2789")
@Builder
public record Statistics(
        Optional<ByteBuffer> max,
        Optional<ByteBuffer> min,
        OptionalLong nullCount,
        OptionalLong distinctCount,
        Optional<ByteBuffer> maxValue,
        Optional<ByteBuffer> minValue,
        boolean isMaxValueExact,
        boolean isMinValueExact) {

    public Statistics {
        max = max == null ? Optional.empty() : max.map(ByteBuffer::asReadOnlyBuffer);
        min = min == null ? Optional.empty() : min.map(ByteBuffer::asReadOnlyBuffer);
        nullCount = nullCount == null ? OptionalLong.empty() : nullCount;
        distinctCount = distinctCount == null ? OptionalLong.empty() : distinctCount;
        maxValue = maxValue == null ? Optional.empty() : maxValue.map(ByteBuffer::asReadOnlyBuffer);
        minValue = minValue == null ? Optional.empty() : minValue.map(ByteBuffer::asReadOnlyBuffer);
    }
}
