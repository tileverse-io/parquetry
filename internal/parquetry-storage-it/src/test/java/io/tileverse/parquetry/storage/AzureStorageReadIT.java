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
package io.tileverse.parquetry.storage;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Properties;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;

/**
 * The storage read against Azure Blob Storage served by Azurite, addressed by the short {@code az://account/container}
 * form with the endpoint overridden to the emulator. The Azure SDK client only creates the blob container; the fixture
 * is uploaded and read through the Storage.
 */
@Testcontainers(disabledWithoutDocker = true)
class AzureStorageReadIT extends AbstractStorageReadIT {

    private static final String ACCOUNT = "devstoreaccount1";

    /** The development account key published by Azurite. */
    private static final String ACCOUNT_KEY =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    private static final String BLOB_CONTAINER = "parquetry-it";

    private static final int BLOB_PORT = 10000;

    @Container
    @SuppressWarnings("resource")
    static AzuriteContainer azurite = new AzuriteContainer("mcr.microsoft.com/azure-storage/azurite:3.37.0");

    @TempDir
    static Path workDir;

    private static Path fixture;

    @BeforeAll
    static void uploadFixture() throws IOException {
        fixture = extractFixture(workDir);
        createBlobContainer();
        putFixture(blobContainer(), azuriteProperties(), fixture);
    }

    private static void createBlobContainer() {
        StorageSharedKeyCredential credential = new StorageSharedKeyCredential(ACCOUNT, ACCOUNT_KEY);
        BlobServiceClient client = new BlobServiceClientBuilder()
                .endpoint(blobEndpoint())
                .credential(credential)
                .buildClient();
        client.getBlobContainerClient(BLOB_CONTAINER).createIfNotExists();
    }

    @Override
    protected URI container() {
        return blobContainer();
    }

    @Override
    protected Properties storageProperties() {
        return azuriteProperties();
    }

    @Override
    protected Path localFixture() {
        return fixture;
    }

    private static URI blobContainer() {
        return URI.create("az://" + ACCOUNT + "/" + BLOB_CONTAINER + "/");
    }

    private static Properties azuriteProperties() {
        Properties properties = new Properties();
        properties.setProperty("storage.azure.endpoint", blobEndpoint());
        properties.setProperty("storage.azure.account-key", ACCOUNT_KEY);
        return properties;
    }

    private static String blobEndpoint() {
        return "http://" + azurite.getHost() + ":" + azurite.getMappedPort(BLOB_PORT) + "/" + ACCOUNT;
    }
}
