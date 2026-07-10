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

import java.net.URI;
import java.net.http.HttpClient;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import io.tileverse.storage.Storage;
import io.tileverse.storage.http.HttpStorageProvider;

/**
 * Runs the backend-agnostic read assertions against the {@code v3_geometry} table served read-only from an Apache httpd
 * container and reached over HTTP range GETs. A plain HTTP file server cannot list, hence the catalog pins the current
 * metadata document explicitly through {@link #catalogOptions()}; every downstream manifest-list, manifest, and data
 * pointer is then an absolute location read by GET without any directory listing.
 *
 * <p>The httpd is started manually in {@link #startHttpd(Path)} because the table tree must be copied into the
 * container before it starts. The Storage borrows the shared {@link HttpClient}; closing a {@link Backend} between
 * tests closes only the Storage, not the client.
 */
@Testcontainers(disabledWithoutDocker = true)
class HttpIcebergReadIT implements IcebergStorageReadAssertions {

    @SuppressWarnings("resource")
    static GenericContainer<?> httpd;

    private static String baseUrl;
    private static HttpClient httpClient;

    @BeforeAll
    static void startHttpd(@TempDir Path tempDir) {
        Path tableDir = IcebergStorageReadAssertions.extractTable(tempDir.resolve(TABLE));
        httpd = new GenericContainer<>(DockerImageName.parse("httpd:alpine"))
                .withExposedPorts(80)
                .withCopyToContainer(MountableFile.forHostPath(tableDir), "/usr/local/apache2/htdocs/" + TABLE)
                .waitingFor(Wait.forHttp("/").forPort(80));
        httpd.start();
        baseUrl = "http://" + httpd.getHost() + ":" + httpd.getFirstMappedPort();
        httpClient = HttpClient.newHttpClient();
    }

    @AfterAll
    static void stop() throws Exception {
        if (httpClient != null) {
            httpClient.close();
        }
        if (httpd != null) {
            httpd.stop();
        }
    }

    /**
     * HTTP cannot list, hence pin the current metadata document explicitly (the logical location the IO maps to a key).
     */
    @Override
    public IcebergOptions catalogOptions() {
        return IcebergOptions.builder()
                .metadataLocation(TABLE_LOCATION + "/metadata/v1.metadata.json")
                .build();
    }

    @Override
    public Backend openBackend() {
        Storage storage = HttpStorageProvider.open(URI.create(baseUrl + "/" + TABLE + "/"), httpClient);
        return new Backend(storage, TABLE_LOCATION);
    }
}
