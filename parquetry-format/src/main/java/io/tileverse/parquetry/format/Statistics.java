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

import java.lang.foreign.MemorySegment;
import java.util.OptionalLong;

import lombok.Builder;

/**
 * Per-column statistics carried in {@link ColumnMetaData}.
 *
 * <p>The (deprecated) {@code max}/{@code min} byte arrays are the legacy fields written by older parquet writers;
 * modern writers use {@code maxValue}/{@code minValue} which obey the column's {@link ColumnOrder}. Both pairs are
 * optional, signalled at the wire-record boundary by {@link MemorySegment#NULL}.
 *
 * <p>All {@link MemorySegment} fields are read-only and index-addressed; consumers cannot mutate the backing bytes nor
 * a shared cursor. Reference identity against {@link MemorySegment#NULL} is the canonical absent test (e.g.
 * {@code stats.max() == MemorySegment.NULL}); legitimately empty payloads come back as zero-byte heap segments that
 * compare unequal to the sentinel.
 *
 * @param max deprecated legacy max bytes; PLAIN-encoded per the column's physical type. {@link MemorySegment#NULL} when
 *     the writer used the newer {@link #maxValue} pair or recorded no max
 * @param min deprecated legacy min bytes; PLAIN-encoded. {@link MemorySegment#NULL} when the writer used
 *     {@link #minValue} or recorded no min
 * @param nullCount number of null values across the row group / page covered by these stats; empty when not recorded
 * @param distinctCount approximate count of distinct non-null values; empty when not recorded
 * @param maxValue PLAIN-encoded max bytes obeying the column's {@link ColumnOrder}; the modern field,
 *     {@link MemorySegment#NULL} when the writer only recorded the legacy {@link #max}
 * @param minValue PLAIN-encoded min bytes obeying the column's {@link ColumnOrder}; the modern field,
 *     {@link MemorySegment#NULL} when the writer only recorded the legacy {@link #min}
 * @param isMaxValueExact {@code true} when {@link #maxValue} is the actual maximum; {@code false} (the conservative
 *     default for absent / explicit-false) means the writer may have rounded up
 * @param isMinValueExact {@code true} when {@link #minValue} is the actual minimum; {@code false} means the writer may
 *     have rounded down
 */
@Builder
public record Statistics(
        MemorySegment max,
        MemorySegment min,
        OptionalLong nullCount,
        OptionalLong distinctCount,
        MemorySegment maxValue,
        MemorySegment minValue,
        boolean isMaxValueExact,
        boolean isMinValueExact) {

    public Statistics {
        max = readOnlyOrAbsent(max);
        min = readOnlyOrAbsent(min);
        nullCount = nullCount == null ? OptionalLong.empty() : nullCount;
        distinctCount = distinctCount == null ? OptionalLong.empty() : distinctCount;
        maxValue = readOnlyOrAbsent(maxValue);
        minValue = readOnlyOrAbsent(minValue);
    }

    private static MemorySegment readOnlyOrAbsent(MemorySegment segment) {
        if (segment == null) {
            return MemorySegment.NULL;
        }
        return segment.isReadOnly() ? segment : segment.asReadOnly();
    }
}
