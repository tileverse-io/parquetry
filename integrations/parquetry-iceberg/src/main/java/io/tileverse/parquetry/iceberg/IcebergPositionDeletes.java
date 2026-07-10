/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.iceberg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Reads an Iceberg positional delete file with parquetry's own reader. A positional delete file is a Parquet file of
 * {@code (file_path, pos)} rows where {@code pos} is an absolute row position within the data file named by
 * {@code file_path}. Because one delete file can name several data files, the rows are grouped by {@code file_path};
 * the delete planner then takes the positions for the data file it is reading.
 */
final class IcebergPositionDeletes {

    private static final ColumnPath FILE_PATH = ColumnPath.of("file_path");
    private static final ColumnPath POS = ColumnPath.of("pos");
    private static final Projection PROJECTION = Projection.ofPhysical(List.of(FILE_PATH, POS));

    private IcebergPositionDeletes() {}

    /** Reads {@code deleteFileLocation} into the deleted positions grouped by the data-file path each row targets. */
    static Map<String, long[]> read(IcebergFileIO io, String deleteFileLocation) {
        Map<String, List<Long>> byDataFile = new HashMap<>();
        try (ByteRangeSource source = io.open(deleteFileLocation)) {
            ParquetSource deletes = ParquetSource.open(source);
            collectPositions(deletes, byDataFile);
        }
        return toArrays(byDataFile);
    }

    private static void collectPositions(ParquetSource deletes, Map<String, List<Long>> byDataFile) {
        try (Stream<ParquetRecord> rows = deletes.read(Predicate.ALWAYS_TRUE, PROJECTION, ReadOptions.DEFAULTS)) {
            rows.forEach(row -> byDataFile
                    .computeIfAbsent(row.getString(FILE_PATH), dataFile -> new ArrayList<>())
                    .add(row.getLong(POS)));
        }
    }

    private static Map<String, long[]> toArrays(Map<String, List<Long>> byDataFile) {
        Map<String, long[]> positions = new HashMap<>();
        byDataFile.forEach((dataFile, rows) -> positions.put(dataFile, toLongArray(rows)));
        return positions;
    }

    private static long[] toLongArray(List<Long> positions) {
        long[] array = new long[positions.size()];
        for (int i = 0; i < array.length; i++) {
            array[i] = positions.get(i);
        }
        return array;
    }
}
