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
package io.tileverse.parquetry.probes;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The optional output-column projection shared by every probe engine, from {@code parquetry.probe.columns} (a
 * comma-separated list of leaf paths, e.g. {@code geometry,subtype,id}). An empty list means read every column.
 * Modelling a query that selects a few attributes rather than the whole row keeps the engines comparable: each reads
 * the same output columns plus whatever its filter needs.
 */
final class ProbeColumns {

    private ProbeColumns() {}

    /** The requested leaf paths, or an empty list to read every column. */
    static List<String> requested() {
        String columns = System.getProperty("parquetry.probe.columns");
        if (columns == null || columns.isBlank()) {
            return List.of();
        }
        return Arrays.stream(columns.split(","))
                .map(String::trim)
                .filter(name -> !name.isEmpty())
                .toList();
    }

    /** The distinct top-level field names of the requested columns (the first path segment of each). */
    static Set<String> requestedTopLevel() {
        return requested().stream().map(name -> name.split("\\.")[0]).collect(Collectors.toSet());
    }
}
