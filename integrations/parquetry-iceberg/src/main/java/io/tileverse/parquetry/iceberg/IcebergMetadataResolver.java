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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * Picks the current metadata-document location for an Iceberg table, using an {@link IcebergFileIO} for all I/O. An
 * explicit override wins; otherwise a {@code version-hint.text} entry is honored when it names an existing version;
 * otherwise the highest {@code vN.metadata.json} under {@code metadata/} is chosen.
 */
final class IcebergMetadataResolver {

    private static final Pattern METADATA_VERSION = Pattern.compile("v(\\d+)\\.metadata\\.json");

    private IcebergMetadataResolver() {}

    static String resolve(IcebergFileIO io, String tableLocation, IcebergOptions options) {
        Optional<String> override = options.metadataLocation();
        if (override.isPresent()) {
            return override.get();
        }
        String metadataDir = stripTrailingSlash(tableLocation) + "/metadata";
        Optional<String> hinted = fromVersionHint(io, metadataDir);
        if (hinted.isPresent()) {
            return hinted.get();
        }
        return highestMetadata(io, metadataDir)
                .orElseThrow(() -> new IcebergFormatException("no metadata.json for table " + tableLocation));
    }

    private static Optional<String> fromVersionHint(IcebergFileIO io, String metadataDir) {
        try {
            String hint = readUtf8(io, metadataDir + "/version-hint.text").trim();
            int version = Integer.parseInt(hint);
            String candidate = metadataDir + "/v" + version + ".metadata.json";
            List<String> entries = io.list(metadataDir + "/");
            return entries.contains(candidate) ? Optional.of(candidate) : Optional.empty();
        } catch (RuntimeException hintUnavailable) {
            return Optional.empty();
        }
    }

    private static Optional<String> highestMetadata(IcebergFileIO io, String metadataDir) {
        String highest = null;
        OptionalInt highestVersion = OptionalInt.empty();
        for (String entry : io.list(metadataDir + "/")) {
            OptionalInt version = metadataVersion(entry);
            if (version.isPresent() && (highestVersion.isEmpty() || version.getAsInt() > highestVersion.getAsInt())) {
                highestVersion = version;
                highest = entry;
            }
        }
        return Optional.ofNullable(highest);
    }

    private static OptionalInt metadataVersion(String location) {
        String basename = basenameOf(location);
        Matcher matcher = METADATA_VERSION.matcher(basename);
        if (!matcher.matches()) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(Integer.parseInt(matcher.group(1)));
    }

    private static String basenameOf(String location) {
        int lastSlash = location.lastIndexOf('/');
        return lastSlash < 0 ? location : location.substring(lastSlash + 1);
    }

    private static String readUtf8(IcebergFileIO io, String location) {
        try (ByteRangeSource source = io.open(location)) {
            return readUtf8(source);
        }
    }

    private static String readUtf8(ByteRangeSource source) {
        long size = source.size();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(size);
            source.readFully(0, segment);
            byte[] bytes = segment.toArray(ValueLayout.JAVA_BYTE);
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    private static String stripTrailingSlash(String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }
}
