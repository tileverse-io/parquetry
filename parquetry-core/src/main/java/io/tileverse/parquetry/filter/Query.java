/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import io.tileverse.parquetry.schema.ColumnPath;

/**
 * The shape of one read: the row filter, the produce-set {@code projection}, the columns to select from it, and an
 * optional row-shaping window.
 *
 * <p>The {@code projection} is the produce set: which columns the read materializes (decode physical, fill constants
 * and typed nulls, rename, widen). {@code outputColumns} then selects and orders the produced columns by name; an empty
 * {@code outputColumns} is the identity, presenting every produced column in produce order. A nested member such as
 * {@code bbox.minx} is just a path in {@code outputColumns}.
 *
 * <p>{@code offset} skips the first {@code offset} rows that satisfy {@code predicate}, and {@code limit} bounds how
 * many rows past that the read returns. Both apply in natural read order, after the exact predicate filter, across
 * single-file and multi-file reads. The identity is {@code offset == 0} and an empty {@code limit}.
 *
 * <p>Build with {@link #of(Predicate, Projection)} for the identity, or {@link #builder(Predicate, Projection)} when
 * selecting output columns or setting a window.
 */
public record Query(
        Predicate predicate, Projection projection, List<ColumnPath> outputColumns, long offset, OptionalLong limit) {

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
        outputColumns = List.copyOf(outputColumns);
    }

    /** A query with the given filter and projection, the identity output selection, and no window. */
    public static Query of(Predicate predicate, Projection projection) {
        return new Query(predicate, projection, List.of(), 0L, OptionalLong.empty());
    }

    /**
     * A builder seeded with the required filter and projection; output selection and window default to the identity.
     */
    public static Builder builder(Predicate predicate, Projection projection) {
        return new Builder(predicate, projection);
    }

    /** Fluent builder for the output selection and the offset/limit window; the filter and projection are required. */
    public static final class Builder {

        private final Predicate predicate;
        private final Projection projection;
        private List<ColumnPath> outputColumns = List.of();
        private long offset = 0L;
        private OptionalLong limit = OptionalLong.empty();

        private Builder(Predicate predicate, Projection projection) {
            this.predicate = predicate;
            this.projection = projection;
        }

        /**
         * Selects and orders the produced columns by name; an empty list keeps the identity (every produced column).
         */
        public Builder outputColumns(List<ColumnPath> outputColumns) {
            this.outputColumns = outputColumns;
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
            return new Query(predicate, projection, outputColumns, offset, limit);
        }
    }
}
