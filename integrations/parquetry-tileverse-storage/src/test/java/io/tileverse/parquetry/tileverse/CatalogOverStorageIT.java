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
package io.tileverse.parquetry.tileverse;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.testkit.TestCorpus;

class CatalogOverStorageIT {

    @Test
    void catalogReadsTwoFileDatasetThroughStorage(@TempDir Path dir) throws Exception {
        // extractFile resolves the file name under the given target DIRECTORY and returns the written file
        Path original = TestCorpus.extractFile("parquet-testing/data/alltypes_plain.parquet", dir);
        Path datasetDir = Files.createDirectory(dir.resolve("ds"));
        Files.copy(original, datasetDir.resolve("a.parquet"));
        Files.copy(original, datasetDir.resolve("b.parquet"));

        long single;
        try (ByteRangeSource src = ByteRangeSource.ofFile(original)) {
            single = ParquetSource.open(src).count();
        }

        Storage storage = StorageFactory.open(datasetDir.toUri());
        try (FilesetCatalog catalog =
                FilesetCatalog.open(StorageFileSource.over(storage, "*.parquet"), CatalogOptions.defaults())) {
            ParquetDataset dataset = catalog.dataset(catalog.datasets().get(0));
            assertThat(dataset.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                    .isEqualTo(2 * single);
        } finally {
            storage.close();
        }
    }
}
