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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.filter.RowPositionSet;
import io.tileverse.parquetry.iceberg.IcebergManifests.DeleteFileRef;
import io.tileverse.parquetry.iceberg.IcebergManifests.Snapshot;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Pins the byte-exact Puffin deletion-vector framing and the clean-room roaring decode against the vendored
 * deletion-vectors fixture. The fixture's Puffin blob removes positions {@code 10..14, 50, 73, 97, 98, 99}; this reads
 * the blob at its manifest content offset and length and asserts the decoder reconstructs exactly that set.
 */
class IcebergDeletionVectorDecodeTest {

    private static final long[] DELETED_POSITIONS = {10, 11, 12, 13, 14, 50, 73, 97, 98, 99};

    private static final long DATA_ROW_COUNT = 100;

    @TempDir
    Path tempDir;

    private final List<AutoCloseable> openResources = new ArrayList<>();

    @AfterEach
    void closeResources() throws Exception {
        for (AutoCloseable resource : openResources) {
            resource.close();
        }
    }

    @Test
    void decodesTheVendoredDeletionVectorToItsExactPositions() throws Exception {
        Fixture fixture = open();
        DeleteFileRef deletionVector = onlyDeletionVector(fixture.snapshot());

        RowPositionSet deleted = IcebergDeletionVectors.read(fixture.io(), deletionVector);

        assertThat(deleted.cardinality()).isEqualTo(DELETED_POSITIONS.length);
        assertEveryDataRowMatchesItsExpectedDeletedState(deleted);
        assertThat(deleted.contains(DATA_ROW_COUNT)).isFalse();
    }

    private static void assertEveryDataRowMatchesItsExpectedDeletedState(RowPositionSet deleted) {
        for (long position = 0; position < DATA_ROW_COUNT; position++) {
            boolean expectedDeleted = isExpectedDeleted(position);
            assertThat(deleted.contains(position))
                    .as("position %d deleted=%b", position, expectedDeleted)
                    .isEqualTo(expectedDeleted);
        }
    }

    private static boolean isExpectedDeleted(long position) {
        return Arrays.binarySearch(DELETED_POSITIONS, position) >= 0;
    }

    @Test
    void readsTheDeletionVectorBlobCoordinatesFromTheManifest() throws Exception {
        Fixture fixture = open();
        DeleteFileRef deletionVector = onlyDeletionVector(fixture.snapshot());

        assertThat(deletionVector.isDeletionVector()).isTrue();
        assertThat(deletionVector.contentOffset()).isEqualTo(4L);
        assertThat(deletionVector.contentSizeInBytes()).isEqualTo(51L);
        assertThat(deletionVector.referencedDataFile())
                .isEqualTo("file:///iceberg-deletes/deletion-vectors/data/data-events.parquet");
    }

    @Test
    void rejectsADeletionVectorWithNoBlobLength() throws Exception {
        Fixture fixture = open();
        IcebergFileIO io = fixture.io();
        DeleteFileRef noLength = deletionVectorWithoutBlobLength();

        assertThatThrownBy(() -> IcebergDeletionVectors.read(io, noLength))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("content_size_in_bytes");
    }

    /**
     * A deletion vector the manifest classifies as such (content 1 with a blob offset) yet omits the blob length. The
     * length guard must reject it before any read; {@link DeleteFileRef#isDeletionVector()} keys only off the offset.
     */
    private static DeleteFileRef deletionVectorWithoutBlobLength() {
        return new DeleteFileRef(
                "file:///iceberg-deletes/deletion-vectors/data/data-events.puffin",
                1,
                2L,
                "file:///iceberg-deletes/deletion-vectors/data/data-events.parquet",
                10L,
                List.of(),
                Map.of(),
                4L,
                null);
    }

    private static DeleteFileRef onlyDeletionVector(Snapshot snapshot) {
        return snapshot.deleteFiles().stream()
                .filter(DeleteFileRef::isDeletionVector)
                .findFirst()
                .orElseThrow();
    }

    private record Fixture(Snapshot snapshot, IcebergFileIO io) {}

    private Fixture open() throws Exception {
        Path tableDir =
                TestCorpus.extractDirectory("iceberg-deletes/deletion-vectors", tempDir.resolve("deletion-vectors"));
        IcebergTableMetadata metadata =
                IcebergTableMetadata.read(Files.readString(tableDir.resolve("metadata/v1.metadata.json")));
        IcebergFileIO io = StorageIcebergFileIO.owning(StorageFactory.open(tableDir.toUri()), metadata.tableLocation());
        openResources.add(io);
        Snapshot snapshot = IcebergManifests.readSnapshot(metadata.manifestListLocation(), io);
        return new Fixture(snapshot, io);
    }
}
