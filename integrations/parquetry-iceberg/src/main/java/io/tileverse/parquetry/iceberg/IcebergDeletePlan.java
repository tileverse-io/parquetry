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

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.RowPositionSet;
import io.tileverse.parquetry.filter.SortedLongPositionSet;
import io.tileverse.parquetry.iceberg.IcebergEqualityDeletes.Tuples;
import io.tileverse.parquetry.iceberg.IcebergManifests.DataFileRef;
import io.tileverse.parquetry.iceberg.IcebergManifests.DeleteFileRef;

/**
 * Resolves which merge-on-read deletes apply to each data file of a snapshot, and turns the applicable deletes into the
 * predicate the read folds into the data file's query. Two delete kinds are resolved:
 *
 * <ul>
 *   <li>Positional deletes drop a set of absolute row positions in a named data file. They become a
 *       {@link RowPositionSet} the read ANDs in as a row-position leaf. A positional delete applies to a data file at
 *       or before its own sequence number that the delete names by path.
 *   <li>Equality deletes drop every row matching a tuple of equality-field values. They become an anti-predicate (see
 *       {@link IcebergEqualityAntiPredicate}) the read ANDs into the data file's predicate. An equality delete applies
 *       strictly to a data file before its own sequence number sharing the same partition.
 * </ul>
 *
 * <p>Because one delete file commonly serves many data files, each delete file is read once and its parsed contents are
 * shared across the parallel per-data-file reads. The plan is metadata-only until a data file is actually read; only
 * then is each applicable delete file parsed (and cached).
 */
final class IcebergDeletePlan {

    private static final IcebergDeletePlan EMPTY = new IcebergDeletePlan(List.of(), List.of(), null, null);

    private final List<DeleteFileRef> positionDeletes;
    private final List<DeleteFileRef> equalityDeletes;
    private final IcebergFileIO io;
    private final IcebergSchema schema;
    private final Map<String, Map<String, long[]>> parsedPositionsByFile = new ConcurrentHashMap<>();
    private final Map<String, Tuples> parsedTuplesByFile = new ConcurrentHashMap<>();

    private IcebergDeletePlan(
            List<DeleteFileRef> positionDeletes,
            List<DeleteFileRef> equalityDeletes,
            IcebergFileIO io,
            IcebergSchema schema) {
        this.positionDeletes = List.copyOf(positionDeletes);
        this.equalityDeletes = List.copyOf(equalityDeletes);
        this.io = io;
        this.schema = schema;
    }

    /**
     * A plan over {@code deleteFiles}, reading them through {@code io} and resolving equality fields against
     * {@code schema}; an empty list yields the no-deletes plan.
     */
    static IcebergDeletePlan of(List<DeleteFileRef> deleteFiles, IcebergFileIO io, IcebergSchema schema) {
        if (deleteFiles.isEmpty()) {
            return EMPTY;
        }
        List<DeleteFileRef> positionDeletes = new ArrayList<>();
        List<DeleteFileRef> equalityDeletes = new ArrayList<>();
        for (DeleteFileRef delete : deleteFiles) {
            if (delete.isEqualityDelete()) {
                equalityDeletes.add(delete);
            } else {
                positionDeletes.add(delete);
            }
        }
        return new IcebergDeletePlan(positionDeletes, equalityDeletes, io, schema);
    }

    /**
     * Whether the snapshot has no merge-on-read deletes at all (the common case; lets the read skip all delete work).
     */
    boolean isEmpty() {
        return positionDeletes.isEmpty() && equalityDeletes.isEmpty();
    }

    /** The deleted row positions that apply to {@code dataFile}, or empty when no positional delete touches it. */
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

    /**
     * The null-safe anti-predicate of every equality delete that applies to {@code dataFile}, or empty when none does.
     * A row survives only when it matches no applicable delete tuple.
     */
    Optional<Predicate> equalityDeletesFor(DataFileRef dataFile) {
        if (equalityDeletes.isEmpty()) {
            return Optional.empty();
        }
        List<Predicate> keepPredicates = new ArrayList<>();
        for (DeleteFileRef delete : equalityDeletes) {
            if (!appliesTo(delete, dataFile)) {
                continue;
            }
            Tuples tuples = tuples(delete);
            antiPredicate(tuples).ifPresent(keepPredicates::add);
        }
        return conjunction(keepPredicates);
    }

    private static Optional<Predicate> antiPredicate(Tuples tuples) {
        return IcebergEqualityAntiPredicate.build(tuples.columns(), tuples.rows());
    }

    private static Optional<Predicate> conjunction(List<Predicate> predicates) {
        if (predicates.isEmpty()) {
            return Optional.empty();
        }
        Predicate combined = predicates.get(0);
        for (int i = 1; i < predicates.size(); i++) {
            combined = combined.and(predicates.get(i));
        }
        return Optional.of(combined);
    }

    private List<long[]> applicablePositions(DataFileRef dataFile) {
        List<long[]> applicable = new ArrayList<>();
        for (DeleteFileRef delete : positionDeletes) {
            if (!appliesTo(delete, dataFile)) {
                continue;
            }
            long[] positions = positions(delete).get(dataFile.location());
            if (positions != null && positions.length > 0) {
                applicable.add(positions);
            }
        }
        return applicable;
    }

    /**
     * Whether {@code delete} can apply to {@code dataFile}. A positional delete applies to data files at or before its
     * own sequence number; a delete that records the single data file it references is also restricted to that path,
     * and whether the data file's rows are actually deleted is then decided by the {@code file_path} column inside the
     * delete file. An equality delete applies strictly to data files before its own sequence number that share the same
     * partition.
     */
    private static boolean appliesTo(DeleteFileRef delete, DataFileRef dataFile) {
        if (delete.isEqualityDelete()) {
            return dataFile.dataSequenceNumber() < delete.dataSequenceNumber()
                    && samePartition(delete.partitionValues(), dataFile.partitionValues());
        }
        if (dataFile.dataSequenceNumber() > delete.dataSequenceNumber()) {
            return false;
        }
        String referenced = delete.referencedDataFile();
        return referenced == null || referenced.equals(dataFile.location());
    }

    /**
     * Whether a delete file's partition tuple matches a data file's. Both tuples are keyed by partition-field-id with
     * null slots omitted; equal maps therefore mean equal partitions (the unpartitioned case is two empty maps).
     */
    private static boolean samePartition(Map<Integer, Object> deletePartition, Map<Integer, Object> dataPartition) {
        return deletePartition.equals(dataPartition);
    }

    private Map<String, long[]> positions(DeleteFileRef delete) {
        return parsedPositionsByFile.computeIfAbsent(
                delete.location(), location -> IcebergPositionDeletes.read(io, location));
    }

    private Tuples tuples(DeleteFileRef delete) {
        return parsedTuplesByFile.computeIfAbsent(
                delete.location(), unusedLocationKey -> IcebergEqualityDeletes.read(io, delete, schema));
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
