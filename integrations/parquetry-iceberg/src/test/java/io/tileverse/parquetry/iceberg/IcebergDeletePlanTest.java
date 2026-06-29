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

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.RowPositionSet;
import io.tileverse.parquetry.iceberg.IcebergManifests.DataFileRef;
import io.tileverse.parquetry.iceberg.IcebergManifests.DeleteFileRef;
import io.tileverse.parquetry.iceberg.IcebergManifests.Snapshot;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Drives the delete planner against the positional and equality delete fixtures. A positional delete applies to the
 * data file it names at or after the data file's sequence number, and to nothing the delete does not name or that is
 * newer than the delete. An equality delete applies strictly to a data file before its own sequence number sharing the
 * same partition, and to nothing at or after that sequence or in a different partition.
 */
class IcebergDeletePlanTest {

    @TempDir
    Path tempDir;

    @Test
    void resolvesEveryDeletedPositionForTheReferencedDataFile() throws Exception {
        Fixture fixture = openPositional();
        IcebergDeletePlan plan = IcebergDeletePlan.of(fixture.snapshot().deleteFiles(), fixture.io(), null);

        RowPositionSet deleted =
                plan.positionsFor(fixture.snapshot().dataFiles().get(0)).orElseThrow();

        assertThat(deleted.cardinality()).isEqualTo(110L);
        assertThat(deleted.contains(300L)).isTrue();
        assertThat(deleted.contains(604L)).isTrue();
        assertThat(deleted.contains(299L)).isFalse();
        assertThat(deleted.contains(605L)).isFalse();
    }

    @Test
    void appliesADeleteWrittenAtTheDataFilesOwnSequence() throws Exception {
        Fixture fixture = openPositional();
        DataFileRef original = fixture.snapshot().dataFiles().get(0);
        long deleteSequence = fixture.snapshot().deleteFiles().get(0).dataSequenceNumber();
        DataFileRef sameSequence = withSequenceNumber(original, deleteSequence);
        IcebergDeletePlan plan = IcebergDeletePlan.of(fixture.snapshot().deleteFiles(), fixture.io(), null);

        assertThat(plan.positionsFor(sameSequence).orElseThrow().cardinality()).isEqualTo(110L);
    }

    @Test
    void appliesNoDeleteToADataFileNewerThanTheDelete() throws Exception {
        Fixture fixture = openPositional();
        DataFileRef original = fixture.snapshot().dataFiles().get(0);
        long afterTheDelete = fixture.snapshot().deleteFiles().get(0).dataSequenceNumber() + 1;
        DataFileRef newer = withSequenceNumber(original, afterTheDelete);
        IcebergDeletePlan plan = IcebergDeletePlan.of(fixture.snapshot().deleteFiles(), fixture.io(), null);

        assertThat(plan.positionsFor(newer)).isEmpty();
    }

    @Test
    void appliesNoDeleteToADataFileTheDeleteDoesNotName() throws Exception {
        Fixture fixture = openPositional();
        DataFileRef original = fixture.snapshot().dataFiles().get(0);
        DataFileRef unreferenced = withLocation(original, "file:///iceberg-deletes/positional/data/other.parquet");
        IcebergDeletePlan plan = IcebergDeletePlan.of(fixture.snapshot().deleteFiles(), fixture.io(), null);

        assertThat(plan.positionsFor(unreferenced)).isEmpty();
    }

    @Test
    void theEmptyPlanAppliesNoDeletes() {
        DataFileRef anyDataFile = new DataFileRef("x", 0L, 0L, Map.of(), Map.of(), Map.of(), Map.of());
        IcebergDeletePlan plan = IcebergDeletePlan.of(List.of(), null, null);

        assertThat(plan.isEmpty()).isTrue();
        assertThat(plan.positionsFor(anyDataFile)).isEmpty();
    }

    @Test
    void anEqualityDeleteDoesNotApplyToADataFileAtItsOwnSequence() {
        DeleteFileRef equalityDelete = equalityDeleteAtSequence(2L);
        DataFileRef sameSequence = dataFileAtSequence(2L);
        IcebergDeletePlan plan = IcebergDeletePlan.of(List.of(equalityDelete), null, null);

        assertThat(plan.equalityDeletesFor(sameSequence)).isEmpty();
    }

    @Test
    void anEqualityDeleteDoesNotApplyToADataFileNewerThanTheDelete() {
        DeleteFileRef equalityDelete = equalityDeleteAtSequence(2L);
        DataFileRef newer = dataFileAtSequence(3L);
        IcebergDeletePlan plan = IcebergDeletePlan.of(List.of(equalityDelete), null, null);

        assertThat(plan.equalityDeletesFor(newer)).isEmpty();
    }

    @Test
    void anEqualityDeleteDoesNotApplyAcrossAPartitionMismatch() {
        DeleteFileRef equalityDelete = equalityDeleteAtSequence(2L, Map.of(1000, "a"));
        DataFileRef otherPartition = dataFileAtSequence(1L, Map.of(1000, "b"));
        IcebergDeletePlan plan = IcebergDeletePlan.of(List.of(equalityDelete), null, null);

        assertThat(plan.equalityDeletesFor(otherPartition)).isEmpty();
    }

    @Test
    void matchesEqualBinaryPartitionValuesByContentNotByMemorySegmentIdentity() {
        MemorySegment deleteValue = segmentOf((byte) 1, (byte) 2, (byte) 3);
        MemorySegment dataValue = segmentOf((byte) 1, (byte) 2, (byte) 3);
        DeleteFileRef equalityDelete = equalityDeleteAtSequence(2L, Map.of(1000, deleteValue));
        DataFileRef sameContentPartition = dataFileAtSequence(1L, Map.of(1000, dataValue));

        assertThat(deleteValue).isNotSameAs(dataValue);
        assertThat(IcebergDeletePlan.appliesToForTest(equalityDelete, sameContentPartition))
                .isTrue();
    }

    @Test
    void doesNotMatchDifferentBinaryPartitionValues() {
        DeleteFileRef equalityDelete = equalityDeleteAtSequence(2L, Map.of(1000, segmentOf((byte) 1, (byte) 2)));
        DataFileRef otherPartition = dataFileAtSequence(1L, Map.of(1000, segmentOf((byte) 1, (byte) 9)));

        assertThat(IcebergDeletePlan.appliesToForTest(equalityDelete, otherPartition))
                .isFalse();
    }

    @Test
    void aGlobalEqualityDeleteAppliesToADataFileWithANonEmptyPartition() {
        DeleteFileRef globalDelete = equalityDeleteAtSequence(3L, Map.of());
        DataFileRef partitioned = dataFileAtSequence(1L, Map.of(1000, "a"));

        assertThat(IcebergDeletePlan.appliesToForTest(globalDelete, partitioned))
                .isTrue();
    }

    @Test
    void anEqualityDeleteAppliesToADataFileStrictlyBeforeItsSequence() throws Exception {
        Fixture fixture = openEquality();
        DataFileRef dataFile = fixture.snapshot().dataFiles().get(0);
        DeleteFileRef equalityDelete = fixture.snapshot().deleteFiles().get(0);
        IcebergDeletePlan plan = IcebergDeletePlan.of(fixture.snapshot().deleteFiles(), fixture.io(), fixture.schema());

        assertThat(dataFile.dataSequenceNumber()).isLessThan(equalityDelete.dataSequenceNumber());
        Optional<Predicate> keep = plan.equalityDeletesFor(dataFile);
        assertThat(keep).isPresent();
    }

    @Test
    void aDeletionVectorAppliesToItsReferencedDataFileAtOrBeforeItsSequence() throws Exception {
        Fixture fixture = openDeletionVectors();
        DeleteFileRef deletionVector = onlyDeletionVector(fixture.snapshot());
        DataFileRef referenced = referencedDataFile(deletionVector, deletionVector.dataSequenceNumber() - 1);
        IcebergDeletePlan plan = IcebergDeletePlan.of(fixture.snapshot().deleteFiles(), fixture.io(), null);

        RowPositionSet deleted = plan.positionsFor(referenced).orElseThrow();

        assertThat(deleted.cardinality()).isEqualTo(10L);
        assertThat(deleted.contains(10L)).isTrue();
        assertThat(deleted.contains(99L)).isTrue();
        assertThat(deleted.contains(0L)).isFalse();
    }

    @Test
    void aDeletionVectorDoesNotApplyToADataFileNewerThanTheDelete() throws Exception {
        Fixture fixture = openDeletionVectors();
        DeleteFileRef deletionVector = onlyDeletionVector(fixture.snapshot());
        DataFileRef newer = referencedDataFile(deletionVector, deletionVector.dataSequenceNumber() + 1);
        IcebergDeletePlan plan = IcebergDeletePlan.of(fixture.snapshot().deleteFiles(), fixture.io(), null);

        assertThat(plan.positionsFor(newer)).isEmpty();
    }

    @Test
    void aDeletionVectorDoesNotApplyToADataFileItDoesNotReference() throws Exception {
        Fixture fixture = openDeletionVectors();
        DeleteFileRef deletionVector = onlyDeletionVector(fixture.snapshot());
        DataFileRef otherFile = dataFileLocatedAt(
                "file:///iceberg-deletes/deletion-vectors/data/other.parquet", deletionVector.dataSequenceNumber() - 1);
        IcebergDeletePlan plan = IcebergDeletePlan.of(fixture.snapshot().deleteFiles(), fixture.io(), null);

        assertThat(plan.positionsFor(otherFile)).isEmpty();
    }

    @Test
    void aDeletionVectorSupersedesAPositionalDeleteFileForTheSameDataFile() throws Exception {
        Fixture fixture = openDeletionVectors();
        DeleteFileRef deletionVector = onlyDeletionVector(fixture.snapshot());
        DataFileRef referenced = referencedDataFile(deletionVector, deletionVector.dataSequenceNumber() - 1);
        DeleteFileRef unreadablePositional =
                positionalDeleteFor(referenced.location(), deletionVector.dataSequenceNumber());
        IcebergDeletePlan plan =
                IcebergDeletePlan.of(List.of(deletionVector, unreadablePositional), fixture.io(), null);

        RowPositionSet deleted = plan.positionsFor(referenced).orElseThrow();

        assertThat(deleted.cardinality()).isEqualTo(10L);
        assertThat(deleted.contains(10L)).isTrue();
    }

    private static DeleteFileRef onlyDeletionVector(Snapshot snapshot) {
        return snapshot.deleteFiles().stream()
                .filter(DeleteFileRef::isDeletionVector)
                .findFirst()
                .orElseThrow();
    }

    private static DataFileRef referencedDataFile(DeleteFileRef deletionVector, long dataSequenceNumber) {
        return dataFileLocatedAt(deletionVector.referencedDataFile(), dataSequenceNumber);
    }

    private static DataFileRef dataFileLocatedAt(String location, long dataSequenceNumber) {
        return new DataFileRef(location, 100L, dataSequenceNumber, Map.of(), Map.of(), Map.of(), Map.of());
    }

    private static DeleteFileRef positionalDeleteFor(String referencedDataFile, long dataSequenceNumber) {
        return new DeleteFileRef(
                "file:///iceberg-deletes/deletion-vectors/data/missing-deletes.parquet",
                1,
                dataSequenceNumber,
                referencedDataFile,
                5L,
                List.of(),
                Map.of(),
                null,
                null);
    }

    private static DeleteFileRef equalityDeleteAtSequence(long dataSequenceNumber) {
        return equalityDeleteAtSequence(dataSequenceNumber, Map.of());
    }

    private static DeleteFileRef equalityDeleteAtSequence(
            long dataSequenceNumber, Map<Integer, Object> partitionValues) {
        return new DeleteFileRef(
                "file:///delete.parquet", 2, dataSequenceNumber, null, 1L, List.of(2, 3), partitionValues, null, null);
    }

    private static MemorySegment segmentOf(byte... bytes) {
        return MemorySegment.ofArray(bytes).asReadOnly();
    }

    private static DataFileRef dataFileAtSequence(long dataSequenceNumber) {
        return dataFileAtSequence(dataSequenceNumber, Map.of());
    }

    private static DataFileRef dataFileAtSequence(long dataSequenceNumber, Map<Integer, Object> partitionValues) {
        return new DataFileRef(
                "file:///data.parquet", 10L, dataSequenceNumber, Map.of(), Map.of(), Map.of(), partitionValues);
    }

    private static DataFileRef withSequenceNumber(DataFileRef ref, long dataSequenceNumber) {
        return new DataFileRef(
                ref.location(),
                ref.recordCount(),
                dataSequenceNumber,
                ref.lowerBounds(),
                ref.upperBounds(),
                ref.nullValueCounts(),
                ref.partitionValues());
    }

    private static DataFileRef withLocation(DataFileRef ref, String location) {
        return new DataFileRef(
                location,
                ref.recordCount(),
                ref.dataSequenceNumber(),
                ref.lowerBounds(),
                ref.upperBounds(),
                ref.nullValueCounts(),
                ref.partitionValues());
    }

    private record Fixture(Snapshot snapshot, IcebergFileIO io, IcebergSchema schema) {}

    private Fixture openPositional() throws Exception {
        return open("positional");
    }

    private Fixture openEquality() throws Exception {
        return open("equality");
    }

    private Fixture openDeletionVectors() throws Exception {
        return open("deletion-vectors");
    }

    private Fixture open(String table) throws Exception {
        Path tableDir = TestCorpus.extractDirectory("iceberg-deletes/" + table, tempDir.resolve(table));
        IcebergTableMetadata metadata =
                IcebergTableMetadata.read(Files.readString(tableDir.resolve("metadata/v1.metadata.json")));
        IcebergFileIO io = new LocalIcebergFileIO(metadata.tableLocation(), tableDir);
        Snapshot snapshot = IcebergManifests.readSnapshot(metadata.manifestListLocation(), io);
        IcebergSchema schema = IcebergSchema.of(metadata.fields());
        return new Fixture(snapshot, io, schema);
    }
}
