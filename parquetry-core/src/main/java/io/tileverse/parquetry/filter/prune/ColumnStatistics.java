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
package io.tileverse.parquetry.filter.prune;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import io.tileverse.parquetry.filter.Value;

/**
 * Typed statistics for one column of a file: decoded min and max values and the null count, each optional. A column
 * with no min/max can still prune null-only predicates through its null count.
 */
public record ColumnStatistics(Optional<Value> min, Optional<Value> max, OptionalLong nullCount) {

    public ColumnStatistics {
        Objects.requireNonNull(min, "min");
        Objects.requireNonNull(max, "max");
        Objects.requireNonNull(nullCount, "nullCount");
    }
}
