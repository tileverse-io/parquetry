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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.iceberg.IcebergManifests.Snapshot;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Reads the merge-on-read delete fixtures' manifests. The positional snapshot references one data file (from the data
 * manifest) and one position-delete file (from the delete manifest); both manifest entries record a null sequence
 * number, leaving each file's data sequence number to be inherited from its manifest. The equality snapshot references
 * one data file and one equality-delete file whose manifest entry records the equality field ids the delete tuples
 * filter on.
 */
class IcebergManifestsDeletesTest {

    @TempDir
    Path tempDir;

    @Test
    void classifiesTheDataFileAndItsPositionDeleteFile() throws Exception {
        Snapshot snapshot = readPositionalSnapshot();

        assertThat(snapshot.dataFiles()).singleElement().satisfies(data -> {
            assertThat(data.recordCount()).isEqualTo(1000L);
            assertThat(data.location()).endsWith(".parquet");
        });
        assertThat(snapshot.deleteFiles()).singleElement().satisfies(delete -> {
            assertThat(delete.recordCount()).isEqualTo(110L);
            assertThat(delete.location()).endsWith("-deletes.parquet");
            assertThat(delete.referencedDataFile()).isNull();
        });
    }

    @Test
    void inheritsEachFilesSequenceNumberFromItsManifest() throws Exception {
        Snapshot snapshot = readPositionalSnapshot();

        long dataSequenceNumber = snapshot.dataFiles().get(0).dataSequenceNumber();
        long deleteSequenceNumber = snapshot.deleteFiles().get(0).dataSequenceNumber();

        assertThat(dataSequenceNumber).isEqualTo(1L);
        assertThat(deleteSequenceNumber).isEqualTo(2L);
    }

    @Test
    void classifiesTheEqualityDeleteFileWithItsEqualityFieldIds() throws Exception {
        Snapshot snapshot = readEqualitySnapshot();

        assertThat(snapshot.deleteFiles()).singleElement().satisfies(delete -> {
            assertThat(delete.isEqualityDelete()).isTrue();
            assertThat(delete.equalityFieldIds()).containsExactly(2, 3);
            assertThat(delete.referencedDataFile()).isNull();
        });
    }

    @Test
    void rejectsADeletionVectorThatNamesNoReferencedDataFile() {
        assertThatThrownBy(() -> IcebergManifests.requireReferencedDataFileForDeletionVectorForTest(128L, null))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("referenced data file");
    }

    @Test
    void acceptsADeletionVectorThatNamesItsReferencedDataFile() {
        assertThatCode(() -> IcebergManifests.requireReferencedDataFileForDeletionVectorForTest(
                        128L, "file:///data/file.parquet"))
                .doesNotThrowAnyException();
    }

    @Test
    void readsANumericEqualityFieldId() {
        assertThat(IcebergManifests.equalityFieldIdForTest(7L)).isEqualTo(7);
    }

    @Test
    void rejectsANonNumericEqualityFieldId() {
        assertThatThrownBy(() -> IcebergManifests.equalityFieldIdForTest("not-a-number"))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("equality_ids");
        assertThatThrownBy(() -> IcebergManifests.equalityFieldIdForTest(null))
                .isInstanceOf(IcebergFormatException.class);
    }

    private Snapshot readPositionalSnapshot() throws Exception {
        return readSnapshot("positional");
    }

    private Snapshot readEqualitySnapshot() throws Exception {
        return readSnapshot("equality");
    }

    private Snapshot readSnapshot(String table) throws Exception {
        Path tableDir = TestCorpus.extractDirectory("iceberg-deletes/" + table, tempDir.resolve(table));
        IcebergTableMetadata metadata =
                IcebergTableMetadata.read(Files.readString(tableDir.resolve("metadata/v1.metadata.json")));
        try (IcebergFileIO io =
                StorageIcebergFileIO.owning(StorageFactory.open(tableDir.toUri()), metadata.tableLocation())) {
            return IcebergManifests.readSnapshot(metadata.manifestListLocation(), io);
        }
    }
}
