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
 * The shape of one read: the row filter, the physical column projection, and the ordered output shape.
 *
 * <p>An empty {@code output} is the identity shape: the read presents the projected physical columns exactly as
 * decoded. A non-empty {@code output} is the complete, ordered list of result columns; each
 * {@link OutputColumn.Physical} or {@link OutputColumn.Promoted} names a physical column the {@code projection}
 * decodes, while constants and typed nulls are filled per row. A future row-shaping addition (offset, limit) lands here
 * without changing the read method signatures.
 */
public record Query(Predicate predicate, Projection projection, List<OutputColumn> output) {

    public Query {
        Objects.requireNonNull(predicate, "predicate");
        Objects.requireNonNull(projection, "projection");
        output = List.copyOf(output);
    }

    /** A query with the given filter and projection and the identity output shape. */
    public static Query of(Predicate predicate, Projection projection) {
        return new Query(predicate, projection, List.of());
    }
}
