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
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.iceberg.IcebergManifests.Snapshot;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Reads the position-delete Parquet file directly and groups the deleted row positions by the data file each row
 * targets. The fixture's delete file removes positions 300..399 (a whole row group) and 595..604 (spanning a row-group
 * boundary) of the single data file.
 */
class IcebergPositionDeletesTest {

    @TempDir
    Path tempDir;

    @Test
    void readsDeletedPositionsGroupedByDataFilePath() throws Exception {
        Path tableDir = TestCorpus.extractDirectory("iceberg-deletes/positional", tempDir.resolve("positional"));
        IcebergTableMetadata metadata =
                IcebergTableMetadata.read(Files.readString(tableDir.resolve("metadata/v1.metadata.json")));
        IcebergFileIO io = new LocalIcebergFileIO(metadata.tableLocation(), tableDir);
        Snapshot snapshot = IcebergManifests.readSnapshot(metadata.manifestListLocation(), io);
        String dataFile = snapshot.dataFiles().get(0).location();
        String deleteFile = snapshot.deleteFiles().get(0).location();

        Map<String, long[]> positionsByDataFile = IcebergPositionDeletes.read(io, deleteFile);

        assertThat(positionsByDataFile).containsOnlyKeys(dataFile);
        long[] positions = positionsByDataFile.get(dataFile);
        assertThat(positions).hasSize(110);
        assertThat(positions).contains(300L, 399L, 595L, 604L);
        assertThat(positions).doesNotContain(299L, 400L, 594L, 605L);
    }
}
