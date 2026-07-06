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
 * The read parameters {@link ArrowOutput#write} applies to a dataset: which rows ({@code predicate}), which columns
 * ({@code projection}), and how many rows ({@code limit}).
 *
 * @param predicate row filter; {@link Predicate#ALWAYS_TRUE} when the caller supplied none
 * @param projection columns to read
 * @param limit maximum number of rows to emit; {@link Long#MAX_VALUE} means unbounded
 */
public record ArrowOutputRequest(Predicate predicate, Projection projection, long limit) {

    public ArrowOutputRequest {
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be >= 0, got " + limit);
        }
    }
}
