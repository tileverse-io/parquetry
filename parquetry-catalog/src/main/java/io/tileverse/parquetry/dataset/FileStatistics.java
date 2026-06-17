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

import java.util.Optional;

import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Name-keyed per-column statistics for one planned file, used by file-level pruning. The first cut exposes the
 * contract; the Iceberg backend populates it from manifest bounds in a later plan.
 */
public interface FileStatistics {

    /** No statistics available; pruning treats the file as a definite candidate. */
    FileStatistics EMPTY = column -> Optional.empty();

    /** Typed min/max/null-count for {@code column}, or empty when unknown. */
    Optional<ColumnStats> forColumn(ColumnPath column);

    /** Minimal typed column statistics. */
    record ColumnStats(Object min, Object max, long nullCount) {}
}
