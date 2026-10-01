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

import java.net.URI;
import java.util.Objects;
import java.util.Properties;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

/**
 * Opens a tileverse-storage {@link Storage} for parquetry's remote reads: every remote read reaches its backend through
 * here, configured by a {@code storage.*} property map. Parquetry adds no defaults of its own, and every backend
 * setting is the deployment's to choose.
 *
 * <p>The byte-range cache is therefore off unless a deployment sets {@code storage.caching.enabled}; leaving it off is
 * tileverse-storage's own default. A read asks for the exact ranges named by its plan - on a decimated read, hundreds
 * of small page ranges per row group - and retaining each of them costs heap needed for decode in a tile-serving pod.
 */
public final class ParquetStorage {

    private ParquetStorage() {}

    /** Opens a {@link Storage} for {@code container} with no backend properties. */
    public static Storage open(URI container) {
        return open(container, new Properties());
    }

    /**
     * Opens a {@link Storage} for {@code container}, configured by the {@code storage.*} entries of {@code properties}.
     */
    public static Storage open(URI container, Properties properties) {
        Objects.requireNonNull(container, "container");
        Objects.requireNonNull(properties, "properties");
        return StorageFactory.open(container, properties);
    }
}
