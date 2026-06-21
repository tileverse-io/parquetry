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
package io.tileverse.parquetry.filter;

import java.util.List;
import java.util.Objects;

/**
 * The shape of one read: the row filter, the physical column projection, and any constant output columns appended after
 * the projected physical columns. A future row-shaping addition (offset, limit) lands here without changing the read
 * method signatures.
 */
public record Query(Predicate predicate, Projection projection, List<ConstantColumn> constantColumns) {

    public Query {
        Objects.requireNonNull(predicate, "predicate");
        Objects.requireNonNull(projection, "projection");
        constantColumns = List.copyOf(constantColumns);
    }

    /** A query with the given filter and projection and no constant columns. */
    public static Query of(Predicate predicate, Projection projection) {
        return new Query(predicate, projection, List.of());
    }
}
