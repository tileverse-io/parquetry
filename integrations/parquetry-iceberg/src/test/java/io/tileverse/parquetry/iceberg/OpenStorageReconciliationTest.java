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

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Proves {@link IcebergTableCatalog#openStorage} opens a remote-style table given only the physical location,
 * reconciling the table's baked-in logical location against the physically-rooted {@link Storage} the same way
 * {@link IcebergTableCatalog#openLocal} does for local tables. The {@code v3_geometry} testbed records a
 * {@code file:///iceberg-geo-testbed/v3_geometry} logical location while the physical bytes live in a temp directory,
 * hence opening it from the physical location exercises the logical-to-physical reconciliation.
 */
class OpenStorageReconciliationTest {

    @Test
    void opensRemoteStyleTableReconcilingLogicalLocation(@TempDir Path tmp) {
        Path tableDir = TestCorpus.extractDirectory("iceberg-geo-testbed", tmp).resolve("v3_geometry");
        Storage storage = StorageFactory.open(tableDir.toUri());
        try (IcebergTableCatalog catalog =
                IcebergTableCatalog.openStorage(tableDir.toUri().toString(), storage, IcebergOptions.defaults())) {
            ParquetDataset dataset = catalog.dataset(catalog.datasets().get(0));
            assertThat(dataset.snapshot()).isPresent();
            assertThat(dataset.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                    .isPositive();
        }
    }
}
