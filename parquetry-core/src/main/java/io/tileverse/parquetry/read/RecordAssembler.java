/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.read;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import io.tileverse.parquetry.materializer.RowAccessor;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.Schema;

/**
 * Dremel record assembler (scope-limited).
 *
 * <p>Walks the projected schema in lockstep with one {@link ColumnReader} per projected leaf column and emits one
 * assembled row at a time as a {@link RowAccessor}.
 *
 * <h2>Supported schema shapes</h2>
 *
 * <ul>
 *   <li>Flat schemas: a top-level group of REQUIRED and OPTIONAL scalar fields.
 *   <li>One level of OPTIONAL group nesting, e.g. an optional group containing scalar fields (each leaf has maxDef=2 if
 *       the group is optional).
 * </ul>
 *
 * <h2>Scope limitation</h2>
 *
 * <p>Columns with a maximum repetition level >= 1 (REPEATED fields or fields nested inside a repeated group) are
 * <em>not</em> supported. The constructor rejects such readers immediately with an
 * {@link UnsupportedOperationException}. Full Dremel repeated/nested array support is deferred to a later iteration
 * alongside the GeoParquet integration tests.
 */
final class RecordAssembler {

    private final Schema schema;
    private final Map<ColumnPath, ColumnReader> readersByPath;

    public RecordAssembler(Schema schema, List<ColumnReader> readers) {
        this.schema = schema;
        this.readersByPath = readers.stream().collect(Collectors.toUnmodifiableMap(ColumnReader::columnPath, r -> r));
        rejectRepeatedColumns(readers);
    }

    /** Returns {@code true} if any reader still has rows to yield. */
    public boolean hasNext() {
        return readersByPath.values().stream().anyMatch(ColumnReader::hasNext);
    }

    /**
     * Assembles and returns the next row by consuming one value from each reader.
     *
     * @throws NoSuchElementException if {@link #hasNext()} is {@code false}.
     */
    public RowAccessor next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more rows to assemble");
        }
        MutableRowAccessor row = new MutableRowAccessor();
        assembleGroup(schema.root(), List.of(), row, /*isRoot*/ true);
        return row;
    }

    // --- schema traversal ---

    /**
     * Walks the children of {@code group}, building their contribution to {@code row}. When {@code isRoot} is true the
     * group's own name is excluded from the path prefix (because the Parquet root group is anonymous in column paths).
     */
    private void assembleGroup(Field.Group group, List<String> ancestorPath, MutableRowAccessor row, boolean isRoot) {
        List<String> groupPath = buildGroupPath(group, ancestorPath, isRoot);
        for (Field child : group.children()) {
            switch (child) {
                case Field.Primitive p -> consumeLeaf(p, groupPath, row);
                case Field.Group g -> consumeNestedGroup(g, groupPath, row);
            }
        }
    }

    private List<String> buildGroupPath(Field.Group group, List<String> ancestorPath, boolean isRoot) {
        if (isRoot) {
            return ancestorPath;
        }
        List<String> path = new ArrayList<>(ancestorPath);
        path.add(group.name());
        return path;
    }

    /**
     * Reads the current value from the leaf's column reader and stores it in {@code row}. When the definition level is
     * below the maximum the leaf is null and no entry is stored.
     */
    private void consumeLeaf(Field.Primitive leaf, List<String> ancestorPath, MutableRowAccessor row) {
        ColumnPath path = buildLeafPath(leaf.name(), ancestorPath);
        ColumnReader reader = readersByPath.get(path);
        if (reader == null) {
            // Column not projected; nothing to store.
            return;
        }
        if (!reader.hasNext()) {
            throw new IllegalStateException("Column " + path.dot() + " exhausted while other columns still have rows");
        }
        boolean isPresent = reader.currentDefinitionLevel() == reader.maxDefinitionLevel();
        if (isPresent) {
            row.values.put(path, reader.currentValue());
        }
        reader.consume();
    }

    /**
     * Delegates assembly of a nested group's children. Null-group tracking is deferred to a later iteration; for now a
     * fully-null optional group simply leaves its descendants absent from {@code row.values}.
     */
    private void consumeNestedGroup(Field.Group group, List<String> ancestorPath, MutableRowAccessor row) {
        assembleGroup(group, ancestorPath, row, /*isRoot*/ false);
    }

    private ColumnPath buildLeafPath(String leafName, List<String> ancestorPath) {
        List<String> parts = new ArrayList<>(ancestorPath);
        parts.add(leafName);
        return new ColumnPath(parts);
    }

    // --- validation ---

    private static void rejectRepeatedColumns(List<ColumnReader> readers) {
        for (ColumnReader reader : readers) {
            if (reader.maxRepetitionLevel() > 0) {
                throw new UnsupportedOperationException(
                        "RecordAssembler does not yet support repeated columns. Column '"
                                + reader.columnPath().dot() + "' has maxRep="
                                + reader.maxRepetitionLevel()
                                + ". Repeated/nested array support is a follow-up task.");
            }
        }
    }
}
