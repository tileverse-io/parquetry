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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.filter.RowPositionSet;
import io.tileverse.parquetry.iceberg.IcebergManifests.DataFileRef;
import io.tileverse.parquetry.iceberg.IcebergManifests.Snapshot;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Drives the delete planner against the positional-delete fixture. The plan must apply the delete file to the data file
 * it names and at or after the data file's sequence number, and apply nothing to a data file the delete does not name
 * or that is newer than the delete.
 */
class IcebergDeletePlanTest {

    @TempDir
    Path tempDir;

    @Test
    void resolvesEveryDeletedPositionForTheReferencedDataFile() throws Exception {
        Fixture fixture = openPositional();
        IcebergDeletePlan plan = IcebergDeletePlan.of(fixture.snapshot().deleteFiles(), fixture.io());

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
        IcebergDeletePlan plan = IcebergDeletePlan.of(fixture.snapshot().deleteFiles(), fixture.io());

        assertThat(plan.positionsFor(sameSequence).orElseThrow().cardinality()).isEqualTo(110L);
    }

    @Test
    void appliesNoDeleteToADataFileNewerThanTheDelete() throws Exception {
        Fixture fixture = openPositional();
        DataFileRef original = fixture.snapshot().dataFiles().get(0);
        long afterTheDelete = fixture.snapshot().deleteFiles().get(0).dataSequenceNumber() + 1;
        DataFileRef newer = withSequenceNumber(original, afterTheDelete);
        IcebergDeletePlan plan = IcebergDeletePlan.of(fixture.snapshot().deleteFiles(), fixture.io());

        assertThat(plan.positionsFor(newer)).isEmpty();
    }

    @Test
    void appliesNoDeleteToADataFileTheDeleteDoesNotName() throws Exception {
        Fixture fixture = openPositional();
        DataFileRef original = fixture.snapshot().dataFiles().get(0);
        DataFileRef unreferenced = withLocation(original, "file:///iceberg-deletes/positional/data/other.parquet");
        IcebergDeletePlan plan = IcebergDeletePlan.of(fixture.snapshot().deleteFiles(), fixture.io());

        assertThat(plan.positionsFor(unreferenced)).isEmpty();
    }

    @Test
    void theEmptyPlanAppliesNoDeletes() {
        DataFileRef anyDataFile = new DataFileRef("x", 0L, 0L, Map.of(), Map.of(), Map.of(), Map.of());
        IcebergDeletePlan plan = IcebergDeletePlan.of(List.of(), null);

        assertThat(plan.isEmpty()).isTrue();
        assertThat(plan.positionsFor(anyDataFile)).isEmpty();
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

    private record Fixture(Snapshot snapshot, IcebergFileIO io) {}

    private Fixture openPositional() throws Exception {
        Path tableDir = TestCorpus.extractDirectory("iceberg-deletes/positional", tempDir.resolve("positional"));
        IcebergTableMetadata metadata =
                IcebergTableMetadata.read(Files.readString(tableDir.resolve("metadata/v1.metadata.json")));
        IcebergFileIO io = new LocalIcebergFileIO(metadata.tableLocation(), tableDir);
        Snapshot snapshot = IcebergManifests.readSnapshot(metadata.manifestListLocation(), io);
        return new Fixture(snapshot, io);
    }
}
