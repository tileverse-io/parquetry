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
package io.tileverse.parquetry.cli.arrow;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;

/**
 * The read parameters {@link ArrowOutput#write} applies to a dataset: which rows ({@code predicate}, plus whether a row
 * filter is actually in play), which columns ({@code projection}), and how many rows ({@code limit}). Grouping these
 * keeps the write signature small and the fast-path/record-path choice readable.
 *
 * @param predicate row filter to apply on the record path; ignored on the fast path
 * @param projection columns to read
 * @param hasFilter whether the caller supplied a row filter; when {@code false} and {@code limit} is unbounded the
 *     columnar fast path is taken
 * @param limit maximum number of rows to emit; {@link Long#MAX_VALUE} means unbounded
 */
public record ArrowOutputRequest(Predicate predicate, Projection projection, boolean hasFilter, long limit) {

    public ArrowOutputRequest {
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be >= 0, got " + limit);
        }
    }
}
