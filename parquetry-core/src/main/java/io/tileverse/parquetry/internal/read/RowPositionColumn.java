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
package io.tileverse.parquetry.internal.read;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import io.tileverse.parquetry.schema.ColumnPath;

/**
 * One synthesized column a row group builds. Without {@code coalesceSource} it is a pure absolute row-position column:
 * {@code name} presents {@code firstRowId} plus each row's within-file position (a positional-delete predicate names a
 * {@code firstRowId} of 0, an Iceberg row id names the data file's {@code first_row_id}).
 *
 * <p>With {@code coalesceSource} it coalesces the decoded physical column at that path per row: a non-null cell keeps
 * its value, a null cell takes the fallback. The fallback is the row position ({@code firstRowId} plus within-file
 * position) when {@code coalesceConstant} is empty, or that constant when present. This models an Iceberg materialized
 * row-lineage column whose stored value wins where present. {@code name} may equal the source (the coalesce fills the
 * source column in place) or differ from it (the filled column presents under a new name).
 */
public record RowPositionColumn(
        ColumnPath name, long firstRowId, Optional<ColumnPath> coalesceSource, OptionalLong coalesceConstant) {

    public RowPositionColumn {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(coalesceSource, "coalesceSource");
        Objects.requireNonNull(coalesceConstant, "coalesceConstant");
    }

    /** A pure absolute row-position column: {@code firstRowId} plus each row's within-file position. */
    public static RowPositionColumn position(ColumnPath name, long firstRowId) {
        return new RowPositionColumn(name, firstRowId, Optional.empty(), OptionalLong.empty());
    }

    /** Coalesce the physical {@code source} with the row position ({@code firstRowId} + within-file position). */
    public static RowPositionColumn coalesceWithPosition(ColumnPath name, ColumnPath source, long firstRowId) {
        return new RowPositionColumn(name, firstRowId, Optional.of(source), OptionalLong.empty());
    }

    /** Coalesce the physical {@code source} with {@code constant} for its null cells. */
    public static RowPositionColumn coalesceWithConstant(ColumnPath name, ColumnPath source, long constant) {
        return new RowPositionColumn(name, 0L, Optional.of(source), OptionalLong.of(constant));
    }

    /** Whether this column coalesces a physical column rather than synthesizing a pure row position. */
    public boolean isCoalesce() {
        return coalesceSource.isPresent();
    }

    /** Whether this coalesce presents under its source column's name, reusing that leaf instead of adding one. */
    public boolean reusesSourceLeaf() {
        return coalesceSource.filter(name::equals).isPresent();
    }
}
