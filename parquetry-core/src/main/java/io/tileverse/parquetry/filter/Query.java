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
import java.util.OptionalLong;

/**
 * The shape of one read: the row filter, the physical column projection, the ordered output shape, and an optional
 * row-shaping window.
 *
 * <p>An empty {@code output} is the identity shape: the read presents the projected physical columns exactly as
 * decoded. A non-empty {@code output} is the complete, ordered list of result columns; each
 * {@link OutputColumn.Physical} or {@link OutputColumn.Promoted} names a physical column the {@code projection}
 * decodes, while constants and typed nulls are filled per row.
 *
 * <p>{@code offset} skips the first {@code offset} rows that satisfy {@code predicate}, and {@code limit} bounds how
 * many rows past that the read returns. Both apply in natural read order, after the exact predicate filter, across
 * single-file and multi-file reads. The identity is {@code offset == 0} and an empty {@code limit}.
 *
 * <p>Build with {@link #of(Predicate, Projection)} for the identity, or {@link #builder(Predicate, Projection)} when
 * setting an output shape or a window.
 */
public record Query(
        Predicate predicate, Projection projection, List<OutputColumn> output, long offset, OptionalLong limit) {

    public Query {
        Objects.requireNonNull(predicate, "predicate");
        Objects.requireNonNull(projection, "projection");
        Objects.requireNonNull(limit, "limit");
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0: " + offset);
        }
        if (limit.isPresent() && limit.getAsLong() < 0) {
            throw new IllegalArgumentException("limit must be >= 0: " + limit.getAsLong());
        }
        output = List.copyOf(output);
    }

    /** A query with the given filter and projection, the identity output shape, and no window. */
    public static Query of(Predicate predicate, Projection projection) {
        return new Query(predicate, projection, List.of(), 0L, OptionalLong.empty());
    }

    /** A builder seeded with the required filter and projection; output shape and window default to the identity. */
    public static Builder builder(Predicate predicate, Projection projection) {
        return new Builder(predicate, projection);
    }

    /** Fluent builder for the output shape and the offset/limit window; the filter and projection are required. */
    public static final class Builder {

        private final Predicate predicate;
        private final Projection projection;
        private List<OutputColumn> output = List.of();
        private long offset = 0L;
        private OptionalLong limit = OptionalLong.empty();

        private Builder(Predicate predicate, Projection projection) {
            this.predicate = predicate;
            this.projection = projection;
        }

        /** Sets the ordered output shape; an empty list keeps the identity shape. */
        public Builder output(List<OutputColumn> output) {
            this.output = output;
            return this;
        }

        /** Skips the first {@code offset} matching rows. */
        public Builder offset(long offset) {
            this.offset = offset;
            return this;
        }

        /** Bounds the read to {@code limit} matching rows past the offset. */
        public Builder limit(long limit) {
            this.limit = OptionalLong.of(limit);
            return this;
        }

        /** Sets the optional row limit. */
        public Builder limit(OptionalLong limit) {
            this.limit = Objects.requireNonNull(limit, "limit");
            return this;
        }

        public Query build() {
            return new Query(predicate, projection, output, offset, limit);
        }
    }
}
