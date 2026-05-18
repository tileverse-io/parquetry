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
import java.util.OptionalInt;
import java.util.OptionalLong;

import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.RowGroup;

/**
 * Deserializer for the Thrift {@code RowGroup} struct.
 *
 * <pre>
 * struct RowGroup {
 *   1: required list&lt;ColumnChunk&gt; columns
 *   2: required i64 total_byte_size
 *   3: required i64 num_rows
 *   4: optional list&lt;SortingColumn&gt; sorting_columns
 *   5: optional i64 file_offset
 *   6: optional i64 total_compressed_size
 *   7: optional i16 ordinal
 * }
 * </pre>
 */
final class RowGroupDeserializer {

    private RowGroupDeserializer() {}

    static RowGroup read(CompactProtocolReader r) throws IOException {
        List<ColumnChunk> columns = new ArrayList<>();
        long totalByteSize = 0;
        long numRows = 0;
        Optional<List<RowGroup.SortingColumn>> sortingColumns = Optional.empty();
        OptionalLong fileOffset = OptionalLong.empty();
        OptionalLong totalCompressedSize = OptionalLong.empty();
        OptionalInt ordinal = OptionalInt.empty();
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) {
                break;
            }
            lastFieldId = fh.fieldId();
            switch (fh.fieldId()) {
                case 1 -> columns = readColumnChunkList(r);
                case 2 -> totalByteSize = r.readI64();
                case 3 -> numRows = r.readI64();
                case 4 -> sortingColumns = Optional.of(readSortingColumnList(r));
                case 5 -> fileOffset = OptionalLong.of(r.readI64());
                case 6 -> totalCompressedSize = OptionalLong.of(r.readI64());
                case 7 -> ordinal = OptionalInt.of(r.readI32());
                default -> r.skipField(fh.type());
            }
        }
        return new RowGroup(columns, totalByteSize, numRows, sortingColumns, fileOffset, totalCompressedSize, ordinal);
    }

    private static List<ColumnChunk> readColumnChunkList(CompactProtocolReader r) throws IOException {
        CompactProtocolReader.ListHeader lh = r.readListHeader();
        List<ColumnChunk> result = new ArrayList<>(lh.size());
        for (int i = 0; i < lh.size(); i++) {
            result.add(ColumnChunkDeserializer.read(r));
        }
        return result;
    }

    private static List<RowGroup.SortingColumn> readSortingColumnList(CompactProtocolReader r) throws IOException {
        CompactProtocolReader.ListHeader lh = r.readListHeader();
        List<RowGroup.SortingColumn> result = new ArrayList<>(lh.size());
        for (int i = 0; i < lh.size(); i++) {
            result.add(SortingColumnDeserializer.read(r));
        }
        return result;
    }
}
