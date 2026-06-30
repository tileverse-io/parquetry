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

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.SequencedSet;
import java.util.Set;

import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * The produce set of one read: which columns the read materializes, and in what order.
 *
 * <p>{@link #ALL} produces every physical leaf of the file, in schema order. {@link Of} is the explicit, ordered list
 * of {@link Column}s to produce: decode a physical column (possibly renamed, reordered, or dropped), widen one to a
 * broader primitive type, fill a constant for every row, or present an all-null typed column. The physical columns a
 * read actually decodes are derived from the produce set; constants and nulls never reach
 * {@link io.tileverse.parquetry.schema.ParquetSchema#project(Set)}.
 */
public sealed interface Projection permits Projection.All, Projection.Of {

    Projection ALL = new All();

    /**
     * A produce set of the given columns, in iteration order.
     *
     * @throws IllegalArgumentException if {@code columns} is empty (use {@link #ALL}) or two columns share a name
     */
    static Projection of(SequencedSet<Column> columns) {
        return new Of(columns);
    }

    /** A pure physical passthrough produce set ({@code name == source}) over {@code leaves}, in iteration order. */
    static Projection ofPhysical(Collection<ColumnPath> leaves) {
        SequencedSet<ColumnPath> distinct = new LinkedHashSet<>(leaves);
        LinkedHashSet<Column> columns = LinkedHashSet.newLinkedHashSet(distinct.size());
        for (ColumnPath leaf : distinct) {
            columns.add(new Column.Physical(leaf, leaf));
        }
        return new Of(columns);
    }

    record All() implements Projection {}

    /** An explicit, ordered produce set: one {@link Column} per produced result column, in iteration order. */
    record Of(SequencedSet<Column> columns) implements Projection {

        public Of {
            columns = new LinkedHashSet<>(columns);
            if (columns.isEmpty()) {
                throw new IllegalArgumentException("Projection.Of must not be empty; use Projection.ALL");
            }
            rejectDuplicateNames(columns);
        }

        /** The physical leaf paths this produce set decodes: the source of every {@link Column.Physical}/Promoted. */
        public Set<ColumnPath> physicalColumns() {
            LinkedHashSet<ColumnPath> sources = new LinkedHashSet<>();
            for (Column column : columns) {
                physicalSource(column).ifPresent(sources::add);
            }
            return sources;
        }

        /** True when producing this set needs shaping beyond a plain physical passthrough (rename, fill, widen). */
        public boolean needsShaping() {
            for (Column column : columns) {
                if (!(column instanceof Column.Physical passthrough)
                        || !passthrough.name().equals(passthrough.source())) {
                    return true;
                }
            }
            return false;
        }

        private static void rejectDuplicateNames(SequencedSet<Column> columns) {
            Set<ColumnPath> seen = HashSet.newHashSet(columns.size());
            for (Column column : columns) {
                if (!seen.add(column.name())) {
                    throw new IllegalArgumentException("duplicate projection column name " + column.name());
                }
            }
        }
    }

    private static Optional<ColumnPath> physicalSource(Column column) {
        return switch (column) {
            case Column.Physical(ColumnPath _, ColumnPath source) -> Optional.of(source);
            case Column.Promoted(ColumnPath _, ColumnPath source, PrimitiveKind _) -> Optional.of(source);
            case Column.Constant _, Column.Null _, Column.RowPosition _ -> Optional.empty();
        };
    }

    /**
     * One column of a read's produced result, in the projection's order.
     *
     * <p>Each {@code Column} describes how to produce one result column: pass a physical column through (possibly
     * renamed, reordered, or dropped), fill a constant value for every row, present an all-null typed column, or widen
     * a physical column to a broader primitive type. Modelling the result this way lets callers express renames,
     * literals, and sanctioned type promotions without the read itself needing to know why each presentation was
     * chosen.
     */
    sealed interface Column permits Column.Physical, Column.Constant, Column.Null, Column.Promoted, Column.RowPosition {

        /** The result column path this column is presented under. */
        ColumnPath name();

        /** Present a decoded physical column under {@link #name()} (projection passthrough, rename, reorder, drop). */
        record Physical(ColumnPath name, ColumnPath source) implements Column {
            public Physical {
                Objects.requireNonNull(name, "name");
                Objects.requireNonNull(source, "source");
            }
        }

        /**
         * Present a constant literal value for every row. Supported types are limited to what {@code ConstantVectors}
         * renders (Int/Long/Double/Float/Bool/Date/String); any other {@link Value} kind throws loudly.
         */
        record Constant(ColumnPath name, Value value) implements Column {
            public Constant {
                Objects.requireNonNull(name, "name");
                Objects.requireNonNull(value, "value");
            }
        }

        /**
         * Present an all-null column; {@code typeOf} selects the column's type (its value is ignored). Supported types
         * are the same set {@code ConstantVectors} renders (Int/Long/Double/Float/Bool/Date/String); any other
         * {@link Value} kind throws loudly.
         */
        record Null(ColumnPath name, Value typeOf) implements Column {
            public Null {
                Objects.requireNonNull(name, "name");
                Objects.requireNonNull(typeOf, "typeOf");
            }
        }

        /**
         * Present a physical column widened to {@code target} (a sanctioned promotion the caller has already
         * validated).
         */
        record Promoted(ColumnPath name, ColumnPath source, PrimitiveKind target) implements Column {
            public Promoted {
                Objects.requireNonNull(name, "name");
                Objects.requireNonNull(source, "source");
                Objects.requireNonNull(target, "target");
            }
        }

        /**
         * Present each row's absolute file position as an {@code INT64} column: {@code firstRowId} plus the row's
         * 0-based physical position in the file, the position kept true under deletes and column-index page-skipping. A
         * deleted or skipped row leaves a gap rather than renumbering its successors. {@code firstRowId} is the
         * caller's per-file base offset: 0 presents the raw within-file position, a file's Iceberg {@code first_row_id}
         * presents a freshly-assigned row id. The position has no physical source; the read synthesizes it.
         */
        record RowPosition(ColumnPath name, long firstRowId) implements Column {
            public RowPosition {
                Objects.requireNonNull(name, "name");
            }
        }
    }
}
