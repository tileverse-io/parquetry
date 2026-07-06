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
package io.tileverse.parquetry.tileverse;

import java.net.URI;
import java.util.Objects;
import java.util.Properties;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

/**
 * Opens a tileverse-storage {@link Storage} tuned for parquetry's read pattern. Because parquetry coalesces a row
 * group's projected column chunks into row-group-sized byte ranges before reading, the storage cache should hold one
 * entry per coalesced range. The tileverse-storage default - a 64 KB block-aligned cache - re-fragments each coalesced
 * range into roughly 128 backend requests; this helper enables caching with block alignment off, caching each native
 * Parquet range as a unit.
 *
 * <p>Caller-supplied properties win: a deployment that explicitly sets {@code storage.caching.enabled} or
 * {@code storage.caching.blockaligned} overrides these defaults.
 */
public final class ParquetStorage {

    private static final String CACHE_ENABLED = "storage.caching.enabled";
    private static final String CACHE_BLOCK_ALIGNED = "storage.caching.blockaligned";

    private ParquetStorage() {}

    /** Opens a {@link Storage} for {@code container} with parquetry's cache defaults and no caller overrides. */
    public static Storage open(URI container) {
        return open(container, new Properties());
    }

    /**
     * Opens a {@link Storage} for {@code container}, applying parquetry's cache defaults beneath {@code properties}.
     * Any caching key present in {@code properties} takes precedence.
     */
    public static Storage open(URI container, Properties properties) {
        Objects.requireNonNull(container, "container");
        Objects.requireNonNull(properties, "properties");
        return StorageFactory.open(container, withParquetDefaults(properties));
    }

    /**
     * Returns a copy of {@code properties} with parquetry's cache defaults filled in for any key the caller left unset:
     * caching enabled, block alignment off. Caller-supplied values are preserved.
     */
    static Properties withParquetDefaults(Properties properties) {
        Properties tuned = new Properties();
        tuned.setProperty(CACHE_ENABLED, "true");
        tuned.setProperty(CACHE_BLOCK_ALIGNED, "false");
        tuned.putAll(properties);
        return tuned;
    }
}
