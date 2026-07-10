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
package io.tileverse.parquetry.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageEntry;
import io.tileverse.storage.UnsupportedCapabilityException;

import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.tileverse.ByteRangeSources;

/**
 * An {@link IcebergFileIO} that serves locations under one logical table URI from a physically-rooted tileverse
 * {@link Storage} (file, S3, Azure, GCS, or HTTP). Iceberg manifests record absolute data-file locations relative to
 * the table's recorded location (the logical root), while the bytes may physically live elsewhere. A location like
 * {@code <logicalRoot>/metadata/v1.metadata.json} maps to the storage key {@code metadata/v1.metadata.json}, which the
 * Storage reads from its physical base. With a key prefix the logical root maps onto that subtree of the storage
 * instead of its root, letting one shared warehouse-rooted Storage serve many tables.
 */
public final class StorageIcebergFileIO implements IcebergFileIO {

    private final Storage storage;
    private final String logicalRoot;
    private final String keyPrefix;
    private final boolean ownsStorage;

    private StorageIcebergFileIO(Storage storage, String logicalRoot, String keyPrefix, boolean ownsStorage) {
        this.storage = Objects.requireNonNull(storage, "storage");
        this.logicalRoot = stripTrailingSlash(Objects.requireNonNull(logicalRoot, "logicalRoot"));
        this.keyPrefix = normalizeKeyPrefix(keyPrefix);
        this.ownsStorage = ownsStorage;
    }

    /** Borrow a caller-owned Storage; {@link #close()} does NOT close it. */
    public static StorageIcebergFileIO over(Storage storage, String logicalRoot) {
        return new StorageIcebergFileIO(storage, logicalRoot, "", false);
    }

    /**
     * A borrowing IO whose logical root maps to the {@code keyPrefix} subtree of {@code storage} instead of its root:
     * {@code <logicalRoot>/x} resolves to the storage key {@code <keyPrefix>/x}. The warehouse catalog uses this to
     * serve every table of a warehouse through one shared Storage.
     */
    static StorageIcebergFileIO over(Storage storage, String logicalRoot, String keyPrefix) {
        return new StorageIcebergFileIO(storage, logicalRoot, keyPrefix, false);
    }

    /** Take ownership of the Storage; {@link #close()} closes it. */
    public static StorageIcebergFileIO owning(Storage storage, String logicalRoot) {
        return new StorageIcebergFileIO(storage, logicalRoot, "", true);
    }

    @Override
    public ByteRangeSource open(String location) {
        Objects.requireNonNull(location, "location");
        RangeReader reader = storage.openRangeReader(keyOf(location));
        return new ReaderOwningByteRangeSource(ByteRangeSources.from(reader), reader);
    }

    @Override
    public List<String> list(String prefix) {
        Objects.requireNonNull(prefix, "prefix");
        return listLogical(listPatternFor(prefix));
    }

    @Override
    public List<String> listMetadataFiles(String rootPrefix) {
        Objects.requireNonNull(rootPrefix, "rootPrefix");
        String base = keyOf(stripTrailingSlash(rootPrefix) + "/");
        return listLogical(base + "**/metadata/*.metadata.json");
    }

    private List<String> listLogical(String pattern) {
        try (Stream<StorageEntry> entries = storage.list(pattern)) {
            return entries.filter(StorageEntry.File.class::isInstance)
                    .map(StorageEntry.File.class::cast)
                    .map(file -> logicalRoot + "/" + unprefixed(file.key()))
                    .sorted()
                    .toList();
        } catch (UnsupportedCapabilityException cannotList) {
            return List.of();
        }
    }

    @Override
    public void close() {
        if (!ownsStorage) {
            return;
        }
        try {
            storage.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close Storage", e);
        }
    }

    private String listPatternFor(String prefix) {
        String key = keyOf(stripTrailingSlash(prefix) + "/");
        return key + "*";
    }

    private String keyOf(String location) {
        String requiredPrefix = logicalRoot + "/";
        if (!location.startsWith(requiredPrefix)) {
            throw new IcebergFormatException("location is outside the table root " + logicalRoot + ": " + location);
        }
        String relative = location.substring(requiredPrefix.length());
        return keyPrefix.isEmpty() ? relative : keyPrefix + "/" + relative;
    }

    private String unprefixed(String key) {
        return keyPrefix.isEmpty() ? key : key.substring(keyPrefix.length() + 1);
    }

    private static String normalizeKeyPrefix(String keyPrefix) {
        Objects.requireNonNull(keyPrefix, "keyPrefix");
        String trimmed = keyPrefix;
        while (trimmed.startsWith("/")) {
            trimmed = trimmed.substring(1);
        }
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }

    private static String stripTrailingSlash(String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

    /**
     * A {@link ByteRangeSource} that owns the {@link RangeReader} the bytes are read through. {@link Storage} hands out
     * a fresh, caller-owned reader per {@code openRangeReader} call; {@link ByteRangeSources#from(RangeReader)} only
     * borrows it. Closing this source closes the reader, honoring the {@link IcebergFileIO#open} own-and-close
     * contract.
     */
    private record ReaderOwningByteRangeSource(ByteRangeSource delegate, RangeReader reader)
            implements ByteRangeSource {

        @Override
        public long size() {
            return delegate.size();
        }

        @Override
        public int read(long offset, MemorySegment dst) {
            return delegate.read(offset, dst);
        }

        @Override
        public void close() {
            try {
                reader.close();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to close RangeReader", e);
            }
        }
    }
}
