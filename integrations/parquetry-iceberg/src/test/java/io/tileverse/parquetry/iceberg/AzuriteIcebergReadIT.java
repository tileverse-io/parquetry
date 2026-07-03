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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;

import io.tileverse.storage.Storage;
import io.tileverse.storage.azure.AzureBlobStorageProvider;

/**
 * Runs the backend-agnostic read assertions against the {@code v3_geometry} table uploaded into an Azure Blob container
 * served by an Azurite emulator. This proves the Iceberg reader resolves metadata, reads manifests and data files, and
 * prunes by manifest bounds over the Azure Blob protocol, while the catalog still uses the table's baked-in logical
 * location.
 *
 * <p>The Storage borrows the shared {@link BlobServiceClient}; closing a {@link Backend} between tests closes only the
 * Storage, not the client. The {@link BlobServiceClient} is not {@code AutoCloseable}, hence there is no client
 * resource to release.
 */
@Testcontainers(disabledWithoutDocker = true)
class AzuriteIcebergReadIT implements IcebergStorageReadAssertions {

    private static final String ACCOUNT_NAME = "devstoreaccount1";
    private static final String ACCOUNT_KEY =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    private static final String CONTAINER = "parquetry-iceberg-it";
    private static final int BLOB_PORT = 10000;

    @Container
    @SuppressWarnings("resource")
    static AzuriteContainer azurite = new AzuriteContainer("mcr.microsoft.com/azure-storage/azurite:3.35.0")
            .withCommand("azurite-blob --skipApiVersionCheck --loose --blobHost 0.0.0.0");

    @TempDir
    static Path corpusDir;

    private static BlobServiceClient blobServiceClient;

    @BeforeAll
    static void uploadTable() {
        Path tableDir = IcebergStorageReadAssertions.extractTable(corpusDir.resolve(TABLE));
        blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(blobEndpoint())
                .credential(new StorageSharedKeyCredential(ACCOUNT_NAME, ACCOUNT_KEY))
                .buildClient();
        blobServiceClient.getBlobContainerClient(CONTAINER).createIfNotExists();
        uploadDirectory(tableDir);
    }

    @Override
    public Backend openBackend() {
        Storage storage =
                AzureBlobStorageProvider.open(URI.create(blobEndpoint() + "/" + CONTAINER), blobServiceClient);
        return new Backend(storage, TABLE_LOCATION);
    }

    private static String blobEndpoint() {
        return "http://" + azurite.getHost() + ":" + azurite.getMappedPort(BLOB_PORT) + "/" + ACCOUNT_NAME;
    }

    private static void uploadDirectory(Path tableDir) {
        try (Stream<Path> files = Files.walk(tableDir)) {
            files.filter(Files::isRegularFile).forEach(file -> uploadFile(tableDir, file));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to walk the table directory " + tableDir, e);
        }
    }

    private static void uploadFile(Path tableDir, Path file) {
        String blobName = relativeBlobName(tableDir, file);
        blobServiceClient
                .getBlobContainerClient(CONTAINER)
                .getBlobClient(blobName)
                .uploadFromFile(file.toString(), true);
    }

    private static String relativeBlobName(Path tableDir, Path file) {
        return tableDir.relativize(file).toString().replace(File.separatorChar, '/');
    }
}
