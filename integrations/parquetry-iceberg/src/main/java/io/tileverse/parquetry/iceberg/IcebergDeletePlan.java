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
package io.tileverse.parquetry.iceberg;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import io.tileverse.parquetry.filter.RowPositionSet;
import io.tileverse.parquetry.filter.SortedLongPositionSet;
import io.tileverse.parquetry.iceberg.IcebergManifests.DataFileRef;
import io.tileverse.parquetry.iceberg.IcebergManifests.DeleteFileRef;

/**
 * Resolves which merge-on-read positional deletes apply to each data file of a snapshot, and turns the applicable
 * deletes into a {@link RowPositionSet} the read folds into the data file's predicate. A delete file applies to a data
 * file when their sequence numbers are compatible and the delete names the data file's path; a positional delete
 * applies to data files at or before its own sequence number.
 *
 * <p>Because one delete file commonly serves many data files, each delete file is read once and its parsed positions
 * are shared across the parallel per-data-file reads. The plan is metadata-only until a data file is actually read;
 * only then is each applicable delete file parsed (and cached).
 */
final class IcebergDeletePlan {

    private static final IcebergDeletePlan EMPTY = new IcebergDeletePlan(List.of(), null);

    private final List<DeleteFileRef> positionDeletes;
    private final IcebergFileIO io;
    private final Map<String, Map<String, long[]>> parsedByDeleteFile = new ConcurrentHashMap<>();

    private IcebergDeletePlan(List<DeleteFileRef> positionDeletes, IcebergFileIO io) {
        this.positionDeletes = List.copyOf(positionDeletes);
        this.io = io;
    }

    /** A plan over {@code deleteFiles}, reading them through {@code io}; an empty list yields the no-deletes plan. */
    static IcebergDeletePlan of(List<DeleteFileRef> deleteFiles, IcebergFileIO io) {
        if (deleteFiles.isEmpty()) {
            return EMPTY;
        }
        return new IcebergDeletePlan(deleteFiles, io);
    }

    /**
     * Whether the snapshot has no merge-on-read deletes at all (the common case; lets the read skip all delete work).
     */
    boolean isEmpty() {
        return positionDeletes.isEmpty();
    }

    /** The deleted row positions that apply to {@code dataFile}, or empty when no delete touches it. */
    Optional<RowPositionSet> positionsFor(DataFileRef dataFile) {
        if (positionDeletes.isEmpty()) {
            return Optional.empty();
        }
        List<long[]> applicable = applicablePositions(dataFile);
        if (applicable.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(SortedLongPositionSet.of(concat(applicable)));
    }

    private List<long[]> applicablePositions(DataFileRef dataFile) {
        List<long[]> applicable = new ArrayList<>();
        for (DeleteFileRef delete : positionDeletes) {
            if (!appliesTo(delete, dataFile)) {
                continue;
            }
            long[] positions = parse(delete).get(dataFile.location());
            if (positions != null && positions.length > 0) {
                applicable.add(positions);
            }
        }
        return applicable;
    }

    /**
     * Whether {@code delete} can apply to {@code dataFile}. A positional delete applies to data files at or before its
     * own sequence number; a delete that records the single data file it references is also restricted to that path.
     * Whether the data file's rows are actually deleted is then decided by the {@code file_path} column inside the
     * delete file.
     */
    private static boolean appliesTo(DeleteFileRef delete, DataFileRef dataFile) {
        if (dataFile.dataSequenceNumber() > delete.dataSequenceNumber()) {
            return false;
        }
        String referenced = delete.referencedDataFile();
        return referenced == null || referenced.equals(dataFile.location());
    }

    private Map<String, long[]> parse(DeleteFileRef delete) {
        return parsedByDeleteFile.computeIfAbsent(
                delete.location(), location -> IcebergPositionDeletes.read(io, location));
    }

    private static long[] concat(List<long[]> arrays) {
        long total = 0L;
        for (long[] array : arrays) {
            total += array.length;
        }
        if (total > Integer.MAX_VALUE) {
            throw new IcebergFormatException("too many deleted positions for one data file: " + total);
        }
        long[] all = new long[(int) total];
        int offset = 0;
        for (long[] array : arrays) {
            System.arraycopy(array, 0, all, offset, array.length);
            offset += array.length;
        }
        return all;
    }
}
