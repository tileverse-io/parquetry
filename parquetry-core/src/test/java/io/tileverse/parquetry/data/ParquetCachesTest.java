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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.testkit.TestCorpus;

import io.tileverse.cache.Cache;
import io.tileverse.cache.CacheManager;
import io.tileverse.cache.CaffeineCache;

/**
 * Pins how far the process-wide reset reaches. Opening a file fills the footer metadata cache, and the reset must drain
 * that one together with every other cache of the shared manager, the storage layer's range cache among them. A
 * local-file read opens no range cache, and a cache registered by the test takes its place.
 */
class ParquetCachesTest {

    private static final String FIXTURE = "parquetry/geo/buildings-gp110-bbox-covering.parquet";

    private static final String ANOTHER_CACHE_NAME = "parquetry-cache-reset-test";

    @TempDir
    Path tempDir;

    @Test
    void clearAllDrainsEveryCacheOfTheSharedManager() {
        FooterMetadataCache.clear();
        Cache<String, String> anotherCache = registeredCache(ANOTHER_CACHE_NAME);
        anotherCache.get("a-byte-range", _ -> "the bytes read for it");

        assertThat(keyCount(FooterMetadataCache.CACHE_NAME))
                .as("the footer cache starts empty")
                .isZero();

        openAReaderOverALocalFile();

        assertThat(keyCount(FooterMetadataCache.CACHE_NAME))
                .as("the open of a named local file fills the footer cache")
                .isPositive();

        ParquetCaches.clearAll();

        assertThat(populatedCacheNames()).isEmpty();
    }

    /** Reads the footer of a corpus file through a source that names itself, which is what the cache keys on. */
    private void openAReaderOverALocalFile() {
        Path file = TestCorpus.extractFile(FIXTURE, tempDir);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            assertThat(reader.schema()).isNotNull();
        }
    }

    /** The cache registered with the shared manager under {@code name}, created empty when there is none yet. */
    private static Cache<String, String> registeredCache(String name) {
        return CacheManager.getDefault()
                .getCache(name, () -> CaffeineCache.<String, String>newBuilder().build());
    }

    /** The names of the caches of the shared manager that hold at least one entry. */
    private static List<String> populatedCacheNames() {
        List<String> populated = new ArrayList<>();
        for (String name : CacheManager.getDefault().getCacheNames()) {
            if (keyCount(name) > 0) {
                populated.add(name);
            }
        }
        return populated;
    }

    /** The number of entries in the cache registered under {@code name}. */
    private static int keyCount(String name) {
        Cache<Object, Object> cache = CacheManager.getDefault().getCache(name, () -> {
            throw new IllegalStateException("the manager lists " + name + " but registers no cache under it");
        });
        Collection<Object> keys = cache.keys();
        return keys.size();
    }
}
