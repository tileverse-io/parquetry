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
import java.util.OptionalInt;
import java.util.OptionalLong;

import lombok.Builder;

/**
 * One horizontal row group within a Parquet file; mirror of {@code RowGroup} in {@code parquet.thrift}.
 *
 * <p>Groups together one {@link ColumnChunk} per leaf column for the same row range. {@link #fileOffset()} and
 * {@link #totalCompressedSize()} let a reader fetch the whole row group with a single byte-range request when present;
 * {@link #sortingColumns()} declares the order in which rows were written.
 *
 * @param columns one {@link ColumnChunk} per leaf column in this row group, in schema order
 * @param totalByteSize total uncompressed byte size of every column chunk in this row group ({@code total_byte_size} in
 *     the thrift schema)
 * @param numRows number of rows in this row group ({@code num_rows} in the thrift schema)
 * @param sortingColumns ordered list of columns the rows are sorted by; empty when the writer did not declare a sort
 *     order
 * @param fileOffset byte offset of the first page of this row group; unset when not recorded ({@code file_offset} in
 *     the thrift schema)
 * @param totalCompressedSize total compressed byte size of every column chunk in this row group; unset when not
 *     recorded ({@code total_compressed_size} in the thrift schema)
 * @param ordinal zero-based ordinal of this row group within the file; unset when not recorded
 */
// S2789: constructor null-tolerates Optional to support the @Builder pattern.
@SuppressWarnings("java:S2789")
@Builder
public record RowGroup(
        List<ColumnChunk> columns,
        long totalByteSize,
        long numRows,
        Optional<List<SortingColumn>> sortingColumns,
        OptionalLong fileOffset,
        OptionalLong totalCompressedSize,
        OptionalInt ordinal) {

    public RowGroup {
        columns = columns == null ? List.of() : List.copyOf(columns);
        sortingColumns = sortingColumns == null ? Optional.empty() : sortingColumns.map(List::copyOf);
        fileOffset = fileOffset == null ? OptionalLong.empty() : fileOffset;
        totalCompressedSize = totalCompressedSize == null ? OptionalLong.empty() : totalCompressedSize;
        ordinal = ordinal == null ? OptionalInt.empty() : ordinal;
    }

    /**
     * One entry in {@link RowGroup#sortingColumns()}: the index of a sorted column plus the sort direction and
     * null-handling.
     *
     * @param columnIdx zero-based index of the sorted column within the row group's column-chunk list
     * @param descending {@code true} when the column is sorted in descending order; {@code false} for ascending
     * @param nullsFirst {@code true} when null values are placed before non-null values; {@code false} when after
     */
    public record SortingColumn(int columnIdx, boolean descending, boolean nullsFirst) {}
}
