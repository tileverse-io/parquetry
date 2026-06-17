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
package io.tileverse.parquetry.dataset;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import io.tileverse.parquetry.io.FileEntry;

/**
 * Groups listed files into {@link DatasetUnit}s by their Hive partition structure ({@code key=value/...} path
 * segments), honoring a maximum partition depth. Files with no partition segments become one unit each
 * (layer-per-file). The listing itself is the discovery mechanism; this class only shapes its result into named
 * datasets.
 */
public final class HivePartitionResolver {

    private static final Pattern INVALID_NC_CHARS = Pattern.compile("[^A-Za-z0-9_.-]");

    private HivePartitionResolver() {}

    /**
     * Resolves {@code files} into dataset units, grouping hive-partitioned files up to {@code maxHiveDepth} levels.
     *
     * @param maxHiveDepth all levels when {@code null}, no levels when {@code 0}; negative is rejected
     */
    public static List<DatasetUnit> resolve(List<FileEntry> files, Integer maxHiveDepth) {
        if (maxHiveDepth != null && maxHiveDepth < 0) {
            throw new IllegalArgumentException("maxHiveDepth is negative: " + maxHiveDepth);
        }
        Map<UnitKey, Grouping> byKey = new LinkedHashMap<>();
        for (FileEntry file : files) {
            List<String[]> all = partitionSegments(file.relativePath());
            List<String[]> retained = truncate(all, maxHiveDepth);
            UnitKey key = unitKey(file.relativePath(), all, retained);
            byKey.computeIfAbsent(key, k -> new Grouping(partitionValues(retained)))
                    .files
                    .add(file);
        }
        List<DatasetUnit> units = new ArrayList<>(byKey.size());
        for (Map.Entry<UnitKey, Grouping> e : byKey.entrySet()) {
            String displayName = toNCName(e.getKey().value());
            units.add(new DatasetUnit(displayName, e.getValue().files, e.getValue().partitionValues));
        }
        return units;
    }

    /** Every {@code [key, value]} partition segment in the path, in order. */
    private static List<String[]> partitionSegments(String relativePath) {
        List<String[]> all = new ArrayList<>();
        for (String part : relativePath.split("/")) {
            int eq = part.indexOf('=');
            boolean hasKeyAndValue = eq > 0 && eq < part.length() - 1;
            if (hasKeyAndValue) {
                all.add(new String[] {part.substring(0, eq), part.substring(eq + 1)});
            }
        }
        return all;
    }

    /**
     * Keeps the first {@code maxHiveDepth} segments: all when {@code null}, fewer only when the cap is below the count.
     */
    private static List<String[]> truncate(List<String[]> all, Integer maxHiveDepth) {
        if (maxHiveDepth != null && maxHiveDepth < all.size()) {
            return new ArrayList<>(all.subList(0, maxHiveDepth));
        }
        return all;
    }

    /**
     * Builds a collision-free grouping key. A path with no partition segments is a layer unit: a top-level file is its
     * own dataset keyed by its filename stem, while files under an immediate subdirectory form one dataset keyed by
     * that subdirectory name. A path truncated to zero retained levels folds into one shared unit; a path with retained
     * levels keeps its {@code key=value} partition path. The kinds cannot collide even when their string values
     * coincide.
     */
    private static UnitKey unitKey(String relativePath, List<String[]> allSegments, List<String[]> retained) {
        if (allSegments.isEmpty()) {
            return layerKey(relativePath);
        }
        if (retained.isEmpty()) {
            return new UnitKey(UnitKind.FOLDED, "");
        }
        return new UnitKey(UnitKind.PARTITION, partitionPath(retained));
    }

    /**
     * Keys a non-hive path by its immediate child of the listing root: a top-level file by its filename stem, a file
     * nested under a subdirectory by that subdirectory name. Keying nested files by the subdirectory keeps same-stem
     * files in different subdirectories ({@code a/data.parquet} vs {@code b/data.parquet}) as distinct datasets instead
     * of silently merging them.
     */
    private static UnitKey layerKey(String relativePath) {
        String[] segments = relativePath.split("/");
        if (segments.length == 1) {
            return new UnitKey(UnitKind.LAYER, stem(segments[0]));
        }
        return new UnitKey(UnitKind.LAYER, segments[0]);
    }

    private static String partitionPath(List<String[]> segments) {
        StringBuilder path = new StringBuilder();
        for (String[] segment : segments) {
            if (!path.isEmpty()) {
                path.append('/');
            }
            path.append(segment[0]).append('=').append(segment[1]);
        }
        return path.toString();
    }

    private static Map<String, String> partitionValues(List<String[]> segments) {
        Map<String, String> values = new LinkedHashMap<>();
        for (String[] segment : segments) {
            values.put(segment[0], segment[1]);
        }
        return values;
    }

    private static String stem(String relativePath) {
        String name = relativePath.substring(relativePath.lastIndexOf('/') + 1);
        int dot = name.lastIndexOf('.');
        return dot > 0 ? name.substring(0, dot) : name;
    }

    /**
     * Converts a grouping key into a valid XML NCName: extract the values of any {@code key=value} segments, join with
     * {@code _}, replace invalid characters with {@code _}, and ensure it starts with a letter or underscore. Returns
     * {@code _} for an empty result. Pre-existing {@code -} and {@code _} are valid NCName characters and are preserved
     * as-is to keep distinct partition values distinct.
     */
    static String toNCName(String input) {
        if (input == null || input.isBlank()) {
            return "_";
        }
        StringBuilder joined = new StringBuilder();
        for (String part : input.split("/")) {
            int eq = part.indexOf('=');
            String token = eq > 0 ? part.substring(eq + 1) : part;
            if (!token.isEmpty()) {
                if (!joined.isEmpty()) {
                    joined.append('_');
                }
                joined.append(token);
            }
        }
        String sanitized = INVALID_NC_CHARS.matcher(joined.toString()).replaceAll("_");
        if (sanitized.isEmpty()) {
            return "_";
        }
        char first = sanitized.charAt(0);
        boolean validStart = Character.isLetter(first) || first == '_';
        return validStart ? sanitized : "_" + sanitized;
    }

    private enum UnitKind {
        LAYER,
        PARTITION,
        FOLDED
    }

    private record UnitKey(UnitKind kind, String value) {}

    private static final class Grouping {
        private final List<FileEntry> files = new ArrayList<>();
        private final Map<String, String> partitionValues;

        private Grouping(Map<String, String> partitionValues) {
            this.partitionValues = partitionValues;
        }
    }
}
