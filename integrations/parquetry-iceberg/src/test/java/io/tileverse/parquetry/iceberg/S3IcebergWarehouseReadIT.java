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

import java.net.URI;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.Storage;
import io.tileverse.storage.s3.S3StorageProvider;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Runs the warehouse-catalog discover-and-read assertions against a two-table warehouse uploaded under one bucket
 * prefix served by an s3proxy container with authorization enabled. This proves {@link IcebergWarehouseCatalog#open}
 * discovers every table through the recursive metadata scan and reads them all through ONE shared Storage over the real
 * S3 protocol with credentials, relocating each table's baked logical root onto its warehouse subtree.
 *
 * <p>The Storage borrows the harness's shared {@code S3Client}; the warehouse catalog owns the Storage and closes it,
 * not the client. The client opens and closes once per class in {@link AbstractS3ProxyIcebergIT}.
 */
class S3IcebergWarehouseReadIT extends AbstractS3ProxyIcebergIT {

    private static final String BUCKET = "parquetry-iceberg-warehouse-it";
    private static final String WAREHOUSE_PREFIX = "warehouse";
    private static final String WAREHOUSE_LOCATION = "s3://" + BUCKET + "/" + WAREHOUSE_PREFIX;

    @TempDir
    static Path corpusDir;

    @BeforeAll
    static void uploadWarehouse() {
        Path warehouse = warehouseWithTwoTables(corpusDir.resolve("wh"));
        createBucket(BUCKET);
        uploadDirectory(warehouse, BUCKET, WAREHOUSE_PREFIX);
    }

    @Test
    void discoversAndReadsThroughASharedStorage() {
        Storage storage = S3StorageProvider.open(URI.create("s3://" + BUCKET + "/" + WAREHOUSE_PREFIX + "/"), s3Client);

        try (IcebergWarehouseCatalog catalog = IcebergWarehouseCatalog.open(WAREHOUSE_LOCATION, storage)) {
            assertThat(catalog.datasets()).containsExactly("ns1.tableA", "tableB");
            assertThat(catalog.dataset("ns1.tableA").count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                    .isEqualTo(15L);
            assertThat(catalog.dataset("tableB").count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                    .isEqualTo(10_000L);
        }
    }

    private static Path warehouseWithTwoTables(Path warehouse) {
        TestCorpus.extractDirectory("iceberg-row-lineage/fresh", warehouse.resolve("ns1/tableA"));
        TestCorpus.extractDirectory("iceberg-geo-testbed/v2_flat_columns", warehouse.resolve("tableB"));
        return warehouse;
    }
}
