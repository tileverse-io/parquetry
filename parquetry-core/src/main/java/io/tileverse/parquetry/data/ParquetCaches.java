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

import io.tileverse.cache.CacheManager;

/**
 * The host-facing operations over the engine's process-wide cache state. What the engine keeps between reads outlives
 * every reader that filled it, and a host resetting itself in place releases it here.
 */
public final class ParquetCaches {

    private ParquetCaches() {}

    /**
     * Discards every cache registered with the shared tileverse {@link CacheManager}: the Parquet footer metadata held
     * by {@link FooterMetadataCache}, and the object byte ranges held by the storage layer. The effect is process-wide,
     * and reaches every reader in the process rather than the readers of one dataset. A reader opened afterwards reads
     * and parses its footer again.
     *
     * <p>Exists for host lifecycle hooks that reset a running process without restarting it. Both caches have to go: a
     * remote object rewritten under the same name and at the same length would otherwise have its stale footer rebuilt
     * out of the byte ranges still held for its previous content.
     */
    public static void clearAll() {
        CacheManager.getDefault().invalidateAll();
    }
}
