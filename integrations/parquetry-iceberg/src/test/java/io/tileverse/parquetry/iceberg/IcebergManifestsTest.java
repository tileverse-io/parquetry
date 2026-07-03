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
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.testkit.TestCorpus;

class IcebergManifestsTest {

    @TempDir
    Path tempDir;

    @Test
    void readsAllDataFilesFromTheManifestList() throws Exception {
        Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir);
        Path tableDir = root.resolve("v2_flat_columns");
        IcebergTableMetadata metadata =
                IcebergTableMetadata.read(Files.readString(tableDir.resolve("metadata/v1.metadata.json")));
        IcebergFileIO io = new LocalIcebergFileIO(metadata.tableLocation(), tableDir);

        List<IcebergManifests.DataFileRef> dataFiles =
                IcebergManifests.readDataFiles(metadata.manifestListLocation(), io);

        assertThat(dataFiles).hasSize(10);
        assertThat(dataFiles).allSatisfy(ref -> assertThat(ref.recordCount()).isEqualTo(1000L));
        assertThat(dataFiles)
                .extracting(IcebergManifests.DataFileRef::location)
                .allMatch(location -> location.endsWith(".parquet"));
    }

    @Test
    void retainsBoundsAndNullCountsPerDataFile() throws Exception {
        Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir);
        Path tableDir = root.resolve("v3_geometry");
        IcebergTableMetadata metadata =
                IcebergTableMetadata.read(Files.readString(tableDir.resolve("metadata/v1.metadata.json")));
        IcebergFileIO io = new LocalIcebergFileIO(metadata.tableLocation(), tableDir);

        List<IcebergManifests.DataFileRef> dataFiles =
                IcebergManifests.readDataFiles(metadata.manifestListLocation(), io);

        assertThat(dataFiles).isNotEmpty();
        IcebergManifests.DataFileRef ref = dataFiles.get(0);

        assertThat(ref.recordCount()).isEqualTo(1000L);
        assertThat(ref.lowerBounds()).isNotEmpty();
        assertThat(ref.upperBounds()).isNotEmpty();

        int geometryFieldId = 2;
        assertThat(ref.lowerBounds()).containsKey(geometryFieldId);
        assertThat(ref.upperBounds()).containsKey(geometryFieldId);
        assertThat(ref.lowerBounds().get(geometryFieldId).byteSize()).isIn(16L, 21L);
        assertThat(ref.upperBounds().get(geometryFieldId).byteSize()).isIn(16L, 21L);
    }

    @Test
    void assignsFirstRowIdByCumulativeWithinManifestInheritance() throws Exception {
        Path root = TestCorpus.extractDirectory("iceberg-row-lineage/fresh", tempDir);
        IcebergTableMetadata metadata =
                IcebergTableMetadata.read(Files.readString(root.resolve("metadata/v1.metadata.json")));
        IcebergFileIO io = new LocalIcebergFileIO(metadata.tableLocation(), root);

        List<IcebergManifests.DataFileRef> dataFiles =
                IcebergManifests.readDataFiles(metadata.manifestListLocation(), io);

        assertThat(dataFiles).hasSize(3);
        assertThat(firstRowIdOf(dataFiles, "data-file-a.parquet")).isZero();
        assertThat(firstRowIdOf(dataFiles, "data-file-b.parquet")).isEqualTo(5L);
        assertThat(firstRowIdOf(dataFiles, "data-file-c.parquet")).isEqualTo(10L);
    }

    private static Long firstRowIdOf(List<IcebergManifests.DataFileRef> dataFiles, String suffix) {
        return dataFiles.stream()
                .filter(ref -> ref.location().endsWith(suffix))
                .map(IcebergManifests.DataFileRef::firstRowId)
                .findFirst()
                .orElseThrow();
    }

    @Test
    void keepsAnExplicitFirstRowIdAndDoesNotAdvanceTheOffsetForIt() {
        List<Long> assigned = IcebergManifests.assignFirstRowIdsForTest(
                1000L, Arrays.asList(800L, null, null), List.of(25L, 50L, 50L));

        assertThat(assigned).containsExactly(800L, 1000L, 1050L);
    }

    @Test
    void assignsNoLineageWhenTheManifestHasNoBase() {
        List<Long> assigned =
                IcebergManifests.assignFirstRowIdsForTest(null, Arrays.asList(null, null), List.of(5L, 5L));

        assertThat(assigned).containsExactly(null, null);
    }

    @Test
    void readsTheIdentityPartitionTuplePerDataFile() throws Exception {
        Path root = TestCorpus.extractDirectory("iceberg-partitioned/by_category", tempDir);
        IcebergTableMetadata metadata =
                IcebergTableMetadata.read(Files.readString(root.resolve("metadata/v1.metadata.json")));
        IcebergFileIO io = new LocalIcebergFileIO(metadata.tableLocation(), root);

        List<IcebergManifests.DataFileRef> dataFiles =
                IcebergManifests.readDataFiles(metadata.manifestListLocation(), io);

        int categoryPartitionFieldId = 1000;
        assertThat(dataFiles).hasSize(3);
        assertThat(dataFiles)
                .allSatisfy(ref -> assertThat(ref.partitionValues()).containsKey(categoryPartitionFieldId));
        assertThat(dataFiles.stream()
                        .map(ref -> ref.partitionValues()
                                .get(categoryPartitionFieldId)
                                .toString())
                        .sorted()
                        .toList())
                .containsExactly("a", "b", "c");
    }
}
