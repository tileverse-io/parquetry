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
package io.tileverse.parquetry.stac;

import java.net.URI;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageConfig;

import io.tileverse.parquetry.tileverse.ParquetStorage;

/**
 * A cache of one tileverse {@link Storage} per container URI, shared across a STAC catalog's document reads and its
 * asset reads. Each container's backend is auto-detected from its own URI (scheme, and an HTTP HEAD probe for ambiguous
 * {@code http(s)} URLs); the supplied connection properties provide credentials, endpoint, and region for whichever
 * provider a container resolves to. A public HTTP catalog and its private object-store assets therefore open side by
 * side under one property set, each backend honoring only the properties that apply to it.
 *
 * <p>A forced {@code storage.provider} (or its legacy {@code io.tileverse.rangereader.provider} alias) is dropped:
 * forcing one provider across every container would misroute an HTTP catalog to the object-store provider. Callers that
 * need a specific backend should address assets with a scheme-bearing URI ({@code s3://}, {@code https://},
 * {@code az://}, {@code gs://}).
 */
public final class ContainerStorages implements AutoCloseable {

    private final Properties connectionProperties;
    private final ConcurrentHashMap<URI, Storage> byContainer = new ConcurrentHashMap<>();

    public ContainerStorages(Properties storageProperties) {
        Objects.requireNonNull(storageProperties, "storageProperties");
        this.connectionProperties = withoutForcedProvider(storageProperties);
    }

    /** The Storage rooted at {@code container}, opened once and reused for every asset and document under it. */
    Storage storageFor(URI container) {
        Objects.requireNonNull(container, "container");
        return byContainer.computeIfAbsent(container, this::openStorage);
    }

    private Storage openStorage(URI container) {
        return ParquetStorage.open(container, connectionProperties);
    }

    private static Properties withoutForcedProvider(Properties source) {
        Properties copy = new Properties();
        copy.putAll(source);
        copy.remove(StorageConfig.PROVIDER_ID_KEY);
        copy.remove(StorageConfig.LEGACY_KEY_PREFIX + "provider");
        return copy;
    }

    @Override
    public void close() {
        RuntimeException failure = null;
        for (Storage storage : byContainer.values()) {
            failure = closeChaining(storage, failure);
        }
        byContainer.clear();
        if (failure != null) {
            throw failure;
        }
    }

    private static RuntimeException closeChaining(AutoCloseable closeable, RuntimeException accumulated) {
        try {
            closeable.close();
            return accumulated;
        } catch (RuntimeException alreadyUnchecked) {
            return chain(accumulated, alreadyUnchecked);
        } catch (Exception checked) {
            return chain(accumulated, new IllegalStateException("closing " + closeable, checked));
        }
    }

    private static RuntimeException chain(RuntimeException accumulated, RuntimeException next) {
        if (accumulated == null) {
            return next;
        }
        accumulated.addSuppressed(next);
        return accumulated;
    }
}
