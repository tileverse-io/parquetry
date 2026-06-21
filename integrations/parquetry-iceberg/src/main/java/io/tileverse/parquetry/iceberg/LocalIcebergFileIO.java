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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * An {@link IcebergFileIO} that serves locations under one logical table URI from a physical local directory. A
 * location is accepted when it starts with {@code logicalRoot}; the remainder resolves against {@code physicalRoot}.
 */
public final class LocalIcebergFileIO implements IcebergFileIO {

    private final String logicalRoot;
    private final Path physicalRoot;

    public LocalIcebergFileIO(String logicalRoot, Path physicalRoot) {
        this.logicalRoot = stripTrailingSlash(Objects.requireNonNull(logicalRoot, "logicalRoot"));
        this.physicalRoot = Objects.requireNonNull(physicalRoot, "physicalRoot");
    }

    @Override
    public ByteRangeSource open(String location) {
        Objects.requireNonNull(location, "location");
        String prefix = logicalRoot + "/";
        if (!location.startsWith(prefix)) {
            throw new IcebergFormatException("location is outside the table root " + logicalRoot + ": " + location);
        }
        String relative = location.substring(prefix.length());
        return ByteRangeSource.ofFile(physicalRoot.resolve(relative));
    }

    @Override
    public List<String> list(String prefix) {
        Objects.requireNonNull(prefix, "prefix");
        Path directory = physicalDirectoryFor(prefix);
        if (!Files.isDirectory(directory)) {
            return List.of();
        }
        String base = stripTrailingSlash(prefix);
        return childLocations(directory, base);
    }

    private Path physicalDirectoryFor(String prefix) {
        String normalized = prefix.equals(logicalRoot) ? logicalRoot + "/" : prefix;
        String requiredPrefix = logicalRoot + "/";
        if (!normalized.startsWith(requiredPrefix)) {
            throw new IcebergFormatException("prefix is outside the table root " + logicalRoot + ": " + prefix);
        }
        String relative = stripTrailingSlash(normalized.substring(requiredPrefix.length()));
        return relative.isEmpty() ? physicalRoot : physicalRoot.resolve(relative);
    }

    private static List<String> childLocations(Path directory, String base) {
        try (Stream<Path> children = Files.list(directory)) {
            List<String> locations = new ArrayList<>();
            children.forEach(child -> locations.add(base + "/" + child.getFileName()));
            locations.sort(String::compareTo);
            return List.copyOf(locations);
        } catch (IOException e) {
            throw new IcebergFormatException("cannot list " + directory, e);
        }
    }

    private static String stripTrailingSlash(String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }
}
