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
package io.tileverse.parquetry.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.RowGroup;

/**
 * Rewrites a flushed row group's column metadata to point at the bloom-filter, column-index, and offset-index blobs the
 * writer placed after it. Pure transforms over the {@code {offset, length}} placements; no I/O.
 */
final class RowGroupPatcher {

    private RowGroupPatcher() {}

    static List<ColumnChunk> patchColumnChunks(
            List<ColumnChunk> sourceChunks,
            List<long[]> bloomPlacements,
            List<long[]> columnIndexPlacements,
            List<long[]> offsetIndexPlacements) {
        List<ColumnChunk> patched = new ArrayList<>(sourceChunks.size());
        for (int i = 0; i < sourceChunks.size(); i++) {
            ColumnChunk source = sourceChunks.get(i);
            ColumnMetaData patchedMeta = patchColumnMetaData(source.metaData().orElseThrow(), bloomPlacements.get(i));
            patched.add(ColumnChunk.builder()
                    .fileOffset(source.fileOffset())
                    .metaData(Optional.of(patchedMeta))
                    .columnIndexOffset(longToOptional(columnIndexPlacements.get(i)[0]))
                    .columnIndexLength(intToOptional(columnIndexPlacements.get(i)[1]))
                    .offsetIndexOffset(longToOptional(offsetIndexPlacements.get(i)[0]))
                    .offsetIndexLength(intToOptional(offsetIndexPlacements.get(i)[1]))
                    .build());
        }
        return patched;
    }

    static RowGroup buildPatchedRowGroup(RowGroup source, List<ColumnChunk> patchedColumns, int ordinal) {
        return RowGroup.builder()
                .columns(patchedColumns)
                .totalByteSize(source.totalByteSize())
                .numRows(source.numRows())
                .fileOffset(source.fileOffset())
                .totalCompressedSize(source.totalCompressedSize())
                .ordinal(OptionalInt.of(ordinal))
                .build();
    }

    private static ColumnMetaData patchColumnMetaData(ColumnMetaData source, long[] bloomPlacement) {
        return ColumnMetaData.builder()
                .type(source.type())
                .encodings(source.encodings())
                .pathInSchema(source.pathInSchema())
                .codec(source.codec())
                .numValues(source.numValues())
                .totalUncompressedSize(source.totalUncompressedSize())
                .totalCompressedSize(source.totalCompressedSize())
                .keyValueMetadata(source.keyValueMetadata())
                .dataPageOffset(source.dataPageOffset())
                .indexPageOffset(source.indexPageOffset())
                .dictionaryPageOffset(source.dictionaryPageOffset())
                .statistics(source.statistics())
                .encodingStats(source.encodingStats())
                .bloomFilterOffset(longToOptional(bloomPlacement[0]))
                .bloomFilterLength(longToOptional(bloomPlacement[1]))
                .sizeStatistics(source.sizeStatistics())
                .geospatialStatistics(source.geospatialStatistics())
                .build();
    }

    private static OptionalLong longToOptional(long value) {
        if (value < 0L) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(value);
    }

    private static OptionalInt intToOptional(long value) {
        if (value < 0L) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(Math.toIntExact(value));
    }
}
