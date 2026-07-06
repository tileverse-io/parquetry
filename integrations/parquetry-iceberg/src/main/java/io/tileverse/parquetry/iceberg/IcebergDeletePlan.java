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
package io.tileverse.parquetry.iceberg;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
 *       strictly to a data file before its own sequence number whose partition it covers; a delete written while the
 *       table was unpartitioned has an empty partition tuple and covers every data file.
 * </ul>
 *
 * <p>Because one delete file commonly serves many data files, each delete file is read once and its parsed contents are
 * shared across the parallel per-data-file reads. The plan is metadata-only until a data file is actually read; only
 * then is each applicable delete file parsed (and cached).
 */
final class IcebergDeletePlan {

    private static final IcebergDeletePlan EMPTY =
            new IcebergDeletePlan(List.of(), List.of(), List.of(), null, null, null);

    private final List<DeleteFileRef> positionDeletes;
    private final List<DeleteFileRef> deletionVectors;
    private final List<DeleteFileRef> equalityDeletes;
    private final IcebergFileIO io;
    private final IcebergSchema schema;
    private final IcebergNameMapping nameMapping;
    private final Map<String, Map<String, long[]>> parsedPositionsByFile = new ConcurrentHashMap<>();
    private final Map<String, RowPositionSet> parsedVectorsByFile = new ConcurrentHashMap<>();
    private final Map<String, Tuples> parsedTuplesByFile = new ConcurrentHashMap<>();
    private final Map<String, Optional<Predicate>> parsedAntiByFile = new ConcurrentHashMap<>();

    private IcebergDeletePlan(
            List<DeleteFileRef> positionDeletes,
            List<DeleteFileRef> deletionVectors,
            List<DeleteFileRef> equalityDeletes,
            IcebergFileIO io,
            IcebergSchema schema,
            IcebergNameMapping nameMapping) {
        this.positionDeletes = List.copyOf(positionDeletes);
        this.deletionVectors = List.copyOf(deletionVectors);
        this.equalityDeletes = List.copyOf(equalityDeletes);
        this.io = io;
        this.schema = schema;
        this.nameMapping = nameMapping;
    }

    /**
     * A plan over {@code deleteFiles}, reading them through {@code io} and resolving equality fields against
     * {@code schema}, with an id-less delete file's columns resolved through {@code nameMapping}; an empty list yields
     * the no-deletes plan.
     */
    static IcebergDeletePlan of(
            List<DeleteFileRef> deleteFiles, IcebergFileIO io, IcebergSchema schema, IcebergNameMapping nameMapping) {
        if (deleteFiles.isEmpty()) {
            return EMPTY;
        }
        List<DeleteFileRef> positionDeletes = new ArrayList<>();
        List<DeleteFileRef> deletionVectors = new ArrayList<>();
        List<DeleteFileRef> equalityDeletes = new ArrayList<>();
        for (DeleteFileRef delete : deleteFiles) {
            classify(delete, positionDeletes, deletionVectors, equalityDeletes);
        }
        return new IcebergDeletePlan(positionDeletes, deletionVectors, equalityDeletes, io, schema, nameMapping);
    }

    private static void classify(
            DeleteFileRef delete,
            List<DeleteFileRef> positionDeletes,
            List<DeleteFileRef> deletionVectors,
            List<DeleteFileRef> equalityDeletes) {
        if (delete.isEqualityDelete()) {
            equalityDeletes.add(delete);
        } else if (delete.isDeletionVector()) {
            deletionVectors.add(delete);
        } else {
            positionDeletes.add(delete);
        }
    }

    /**
     * Whether the snapshot has no merge-on-read deletes at all (the common case; lets the read skip all delete work).
     */
    boolean isEmpty() {
        return positionDeletes.isEmpty() && deletionVectors.isEmpty() && equalityDeletes.isEmpty();
    }

    /**
     * The deleted row positions that apply to {@code dataFile}, or empty when no positional delete touches it. A
     * deletion vector supersedes positional delete files for the same data file: when one applies, its positions are
     * the only positional deletes used and the positional delete files are ignored.
     */
    Optional<RowPositionSet> positionsFor(DataFileRef dataFile) {
        Optional<RowPositionSet> vector = applicableVector(dataFile);
        if (vector.isPresent()) {
            return vector;
        }
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
     * The deletion vector that applies to {@code dataFile}, decoded once and cached, or empty when none does. A
     * deletion vector applies to the single data file it references at or before its own sequence number, the same rule
     * a positional delete file follows.
     */
    private Optional<RowPositionSet> applicableVector(DataFileRef dataFile) {
        for (DeleteFileRef deletionVector : deletionVectors) {
            if (appliesTo(deletionVector, dataFile)) {
                // Iceberg writes at most one deletion vector per data file in a snapshot (a later vector replaces the
                // earlier one), hence the first match is the only one that applies.
                return Optional.of(vector(deletionVector));
            }
        }
        return Optional.empty();
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
            antiPredicate(delete).ifPresent(keepPredicates::add);
        }
        return conjunction(keepPredicates);
    }

    /**
     * The anti-predicate of one equality-delete file, built once from its parsed tuples and cached. One delete file
     * commonly applies to many data files; the conjunction across applicable files stays per data file, but the
     * per-file build (size O(tuples * fields)) happens only once.
     */
    private Optional<Predicate> antiPredicate(DeleteFileRef delete) {
        return parsedAntiByFile.computeIfAbsent(equalityCacheKey(delete), unusedKey -> {
            Tuples tuples = tuples(delete);
            return IcebergEqualityAntiPredicate.build(tuples.columns(), tuples.rows());
        });
    }

    /**
     * Keys an equality-delete file by its location and equality field ids. Two delete files sharing a location but
     * differing in equality fields are distinct entries the same way the deletion-vector cache distinguishes blobs by
     * location and offset.
     */
    private static String equalityCacheKey(DeleteFileRef delete) {
        return delete.location() + "#" + delete.equalityFieldIds();
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
     * delete file. An equality delete applies strictly to data files before its own sequence number whose partition it
     * covers.
     */
    private static boolean appliesTo(DeleteFileRef delete, DataFileRef dataFile) {
        if (delete.isEqualityDelete()) {
            return dataFile.dataSequenceNumber() < delete.dataSequenceNumber() && coversPartition(delete, dataFile);
        }
        if (dataFile.dataSequenceNumber() > delete.dataSequenceNumber()) {
            return false;
        }
        String referenced = delete.referencedDataFile();
        return referenced == null || referenced.equals(dataFile.location());
    }

    /** Test hook: whether {@code delete} can apply to {@code dataFile} by sequence, reference, and partition. */
    static boolean appliesToForTest(DeleteFileRef delete, DataFileRef dataFile) {
        return appliesTo(delete, dataFile);
    }

    /**
     * Whether an equality delete covers a data file's partition. A delete written while the table was unpartitioned has
     * an empty partition tuple and is global: it covers every data file regardless of the file's partition. A delete
     * with a non-empty partition covers only data files in the matching partition.
     */
    private static boolean coversPartition(DeleteFileRef delete, DataFileRef dataFile) {
        if (delete.partitionValues().isEmpty()) {
            return true;
        }
        return samePartition(delete.partitionValues(), dataFile.partitionValues());
    }

    /**
     * Whether a delete file's partition tuple matches a data file's. Both tuples are keyed by partition-field-id with
     * null slots omitted; equal keys with equal values therefore mean equal partitions (the unpartitioned case is two
     * empty maps).
     *
     * <p>The comparison is value-based, not {@link Map#equals(Object)}: a binary or fixed identity-partition column
     * decodes to a read-only {@link MemorySegment} whose {@code equals} is identity-based, which would make two
     * logically-equal partitions compare unequal and the equality delete be skipped.
     */
    private static boolean samePartition(Map<Integer, Object> deletePartition, Map<Integer, Object> dataPartition) {
        if (deletePartition.size() != dataPartition.size()) {
            return false;
        }
        for (Map.Entry<Integer, Object> entry : deletePartition.entrySet()) {
            if (!dataPartition.containsKey(entry.getKey())) {
                return false;
            }
            if (!sameValue(entry.getValue(), dataPartition.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether two partition values are equal, treating two {@link MemorySegment} values as equal when their contents
     * match. {@link MemorySegment#equals(Object)} is identity-based; comparing the bytes is what a binary or fixed
     * identity-partition column needs.
     */
    private static boolean sameValue(Object left, Object right) {
        if (left instanceof MemorySegment leftSegment && right instanceof MemorySegment rightSegment) {
            return leftSegment.byteSize() == rightSegment.byteSize() && leftSegment.mismatch(rightSegment) == -1;
        }
        return Objects.equals(left, right);
    }

    private Map<String, long[]> positions(DeleteFileRef delete) {
        return parsedPositionsByFile.computeIfAbsent(
                delete.location(), location -> IcebergPositionDeletes.read(io, location));
    }

    /**
     * Caches the decoded vector per blob. One Puffin file holds a separate blob per referenced data file, all at the
     * same location but distinct offsets, hence the location-and-offset key.
     */
    private RowPositionSet vector(DeleteFileRef deletionVector) {
        String blobKey = deletionVector.location() + "#" + deletionVector.contentOffset();
        return parsedVectorsByFile.computeIfAbsent(
                blobKey, unusedBlobKey -> IcebergDeletionVectors.read(io, deletionVector));
    }

    private Tuples tuples(DeleteFileRef delete) {
        return parsedTuplesByFile.computeIfAbsent(
                equalityCacheKey(delete), unusedKey -> IcebergEqualityDeletes.read(io, delete, schema, nameMapping));
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
