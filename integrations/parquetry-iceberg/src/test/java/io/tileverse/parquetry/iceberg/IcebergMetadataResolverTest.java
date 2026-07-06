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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.StorageFactory;

class IcebergMetadataResolverTest {

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
    void picksHighestVersionWhenNoHint() throws Exception {
        Path tableDir = tableWithMetadataVersions(1, 2);
        IcebergFileIO bootstrap = bootstrapFor(tableDir);

        String resolved =
                IcebergMetadataResolver.resolve(bootstrap, tableLocation(tableDir), IcebergOptions.defaults());

        assertThat(resolved).endsWith("/metadata/v2.metadata.json");
    }

    @Test
    void versionHintWinsOverHighestVersion() throws Exception {
        Path tableDir = tableWithMetadataVersions(1, 2);
        writeVersionHint(tableDir, "  1\n");
        IcebergFileIO bootstrap = bootstrapFor(tableDir);

        String resolved =
                IcebergMetadataResolver.resolve(bootstrap, tableLocation(tableDir), IcebergOptions.defaults());

        assertThat(resolved).endsWith("/metadata/v1.metadata.json");
    }

    @Test
    void versionHintSelectsTheNamedVersion() throws Exception {
        Path tableDir = tableWithMetadataVersions(1, 2);
        writeVersionHint(tableDir, "2");
        IcebergFileIO bootstrap = bootstrapFor(tableDir);

        String resolved =
                IcebergMetadataResolver.resolve(bootstrap, tableLocation(tableDir), IcebergOptions.defaults());

        assertThat(resolved).endsWith("/metadata/v2.metadata.json");
    }

    @Test
    void explicitOverrideWinsOverEverything() throws Exception {
        Path tableDir = tableWithMetadataVersions(1, 2);
        writeVersionHint(tableDir, "1");
        IcebergFileIO bootstrap = bootstrapFor(tableDir);
        IcebergOptions options = IcebergOptions.builder()
                .metadataLocation("file:///custom/v9.metadata.json")
                .build();

        String resolved = IcebergMetadataResolver.resolve(bootstrap, tableLocation(tableDir), options);

        assertThat(resolved).isEqualTo("file:///custom/v9.metadata.json");
    }

    @Test
    void hintNamingAMissingVersionFallsThroughToHighest() throws Exception {
        Path tableDir = tableWithMetadataVersions(1, 2);
        writeVersionHint(tableDir, "5");
        IcebergFileIO bootstrap = bootstrapFor(tableDir);

        String resolved =
                IcebergMetadataResolver.resolve(bootstrap, tableLocation(tableDir), IcebergOptions.defaults());

        assertThat(resolved).endsWith("/metadata/v2.metadata.json");
    }

    @Test
    void failsWhenNothingResolvable() throws Exception {
        Path tableDir = tempDir.resolve("empty");
        Files.createDirectories(tableDir.resolve("metadata"));
        IcebergFileIO bootstrap = bootstrapFor(tableDir);

        assertThatThrownBy(() ->
                        IcebergMetadataResolver.resolve(bootstrap, tableLocation(tableDir), IcebergOptions.defaults()))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining(tableLocation(tableDir));
    }

    @Test
    void listReturnsMetadataEntriesThatRoundTripThroughOpen() throws Exception {
        Path tableDir = tableWithMetadataVersions(1, 2);
        IcebergFileIO io = bootstrapFor(tableDir);

        List<String> entries = io.list(tableDir.toUri() + "metadata/");

        assertThat(entries).hasSize(2);
        assertThat(entries).anySatisfy(entry -> assertThat(entry).endsWith("/v1.metadata.json"));
        assertThat(entries).anySatisfy(entry -> assertThat(entry).endsWith("/v2.metadata.json"));
        for (String entry : entries) {
            io.open(entry).close();
        }
    }

    private Path tableWithMetadataVersions(int... versions) throws Exception {
        Path tableDir = tempDir.resolve("table");
        Path metadataDir = Files.createDirectories(tableDir.resolve("metadata"));
        for (int version : versions) {
            Files.write(metadataDir.resolve("v" + version + ".metadata.json"), "{}".getBytes(StandardCharsets.UTF_8));
        }
        return tableDir;
    }

    private void writeVersionHint(Path tableDir, String content) throws Exception {
        Files.write(
                tableDir.resolve("metadata").resolve("version-hint.text"), content.getBytes(StandardCharsets.UTF_8));
    }

    private IcebergFileIO bootstrapFor(Path tableDir) {
        IcebergFileIO io = StorageIcebergFileIO.owning(StorageFactory.open(tableDir.toUri()), tableLocation(tableDir));
        openResources.add(io);
        return io;
    }

    private String tableLocation(Path tableDir) {
        return tableDir.toUri().toString();
    }
}
