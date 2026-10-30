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
package io.tileverse.parquetry.data;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SequencedSet;
import java.util.Set;

import io.tileverse.parquetry.columnar.OutputBatches;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.internal.read.RowPositionColumn;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ParquetSchemaException;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * The resolved projection for one read: the columns synthesized by the read, whether it evaluates the predicate on each
 * row, the {@link Projection} the filter pipeline and decode run over (physical leaves only), the physical output
 * schema surviving batches are narrowed to, the produced result schema the read materializes, and the produce step that
 * shapes a decoded batch into the produce set. The projection-resolution logic lives here, out of
 * {@link ParquetFileReader}.
 *
 * <p>The synthesized columns are the row positions named by the predicate and the row-position and coalesce outputs of
 * the produce set. The physical output schema ends with one leaf per synthesized output presented under a new name, and
 * the decode fills that leaf from {@link #synthesizedColumns}. Both come from the same produce set, resolved here.
 *
 * <p>A produce set that names no physical column (only constants and nulls) still needs one decoded column to enumerate
 * the file's rows. {@link #resolve} keeps one such column in the decode and narrow sets - a predicate column when the
 * filter has one, else a cheap leaf - and {@link #produce} drops it from the produced result.
 */
final class ProjectionPlan {

    private final List<RowPositionColumn> synthesizedColumns;
    private final boolean recordLevel;
    private final Projection scanProjection;
    private final ParquetSchema physicalOutputSchema;
    private final ParquetSchema producedSchema;
    private final Optional<Projection.Of> shaping;

    private ProjectionPlan(
            List<RowPositionColumn> synthesizedColumns,
            boolean recordLevel,
            Projection scanProjection,
            ParquetSchema physicalOutputSchema,
            ParquetSchema producedSchema,
            Optional<Projection.Of> shaping) {
        this.synthesizedColumns = synthesizedColumns;
        this.recordLevel = recordLevel;
        this.scanProjection = scanProjection;
        this.physicalOutputSchema = physicalOutputSchema;
        this.producedSchema = producedSchema;
        this.shaping = shaping;
    }

    /**
     * Resolves the projection of a read of {@code projection} filtered by {@code predicate}. A read evaluating the
     * predicate on each row also scans the columns of the predicate.
     *
     * @param useRecordLevelFilter whether the read was asked to evaluate the predicate on each row. A read synthesizing
     *     a column evaluates it in any case.
     * @throws ParquetSchemaException when a synthesized column has a name the reader cannot produce
     */
    static ProjectionPlan resolve(
            ParquetSchema fileSchema, Projection projection, Predicate predicate, boolean useRecordLevelFilter) {
        List<RowPositionColumn> synthesized = synthesizedColumns(fileSchema, predicate, projection);
        boolean recordLevel = useRecordLevelFilter || !synthesized.isEmpty();
        Predicate scanned = recordLevel ? predicate : Predicate.ALWAYS_TRUE;
        return switch (projection) {
            case Projection.All _ ->
                new ProjectionPlan(synthesized, recordLevel, Projection.ALL, fileSchema, fileSchema, Optional.empty());
            case Projection.Of of -> resolveOf(fileSchema, of, scanned, synthesized, recordLevel);
        };
    }

    /**
     * The row-position columns synthesized for the positional leaves of {@code predicate}, each presenting a position
     * within the file. A read producing no column, such as a count, synthesizes these alone.
     *
     * @throws ParquetSchemaException when a positional leaf names a column the reader cannot produce
     */
    static List<RowPositionColumn> rowPositionColumns(ParquetSchema fileSchema, Predicate predicate) {
        return synthesizedColumns(fileSchema, predicate, Projection.ALL);
    }

    private static ProjectionPlan resolveOf(
            ParquetSchema fileSchema,
            Projection.Of of,
            Predicate scanned,
            List<RowPositionColumn> synthesized,
            boolean recordLevel) {
        Set<ColumnPath> physical = of.physicalColumns();
        Set<ColumnPath> decodeSet = new LinkedHashSet<>(physical);
        decodeSet.addAll(Predicate.columns(scanned));
        // A predicate may reference synthesized row-position columns (a positional-delete leaf); the reader produces
        // those rather than decoding them, and ParquetSchema.project must never receive them.
        Predicate.rowPositionColumns(scanned).forEach(decodeSet::remove);
        if (decodeSet.isEmpty()) {
            decodeSet.add(fileSchema.leafColumns().get(0));
        }
        Set<ColumnPath> outputPhysical =
                physical.isEmpty() ? Set.of(decodeSet.iterator().next()) : physical;
        Projection scan = Projection.ofPhysical(decodeSet);
        ParquetSchema physicalOutput = fileSchema.project(outputPhysical).withAppendedLeaves(rowPositionLeaves(of));
        if (of.needsShaping()) {
            ParquetSchema produced = OutputBatches.producedSchema(fileSchema, of);
            return new ProjectionPlan(synthesized, recordLevel, scan, physicalOutput, produced, Optional.of(of));
        }
        return new ProjectionPlan(synthesized, recordLevel, scan, physicalOutput, physicalOutput, Optional.empty());
    }

    /**
     * The synthesized output leaves of {@code of}: its row-position outputs plus any coalesce presented under a name
     * other than its source (a coalesce under its source name reuses that physical leaf). The record-level filter
     * narrows a surviving batch to this schema; appending these leaves keeps the decode-time synthesized vectors
     * through that narrow step and into {@link #produce}. A produce set with no synthesized output appends nothing.
     */
    private static List<SchemaNode.Primitive> rowPositionLeaves(Projection.Of of) {
        List<SchemaNode.Primitive> leaves = new ArrayList<>();
        for (Projection.Column column : of.columns()) {
            switch (column) {
                case Projection.Column.RowPosition(ColumnPath name, long _) ->
                    leaves.add(OutputBatches.rowPositionLeaf(name.name(), -1));
                case Projection.Column.Coalesce(
                        ColumnPath name,
                        ColumnPath source,
                        Projection.Column.Coalesce.Fallback _)
                when !name.equals(source) -> leaves.add(OutputBatches.rowPositionLeaf(name.name(), -1));
                default -> {
                    // physical, promoted, constant and null columns, and a coalesce under its source name,
                    // synthesize nothing
                }
            }
        }
        return leaves;
    }

    /**
     * The columns synthesized by a read: the positional columns of the predicate, each a position within the file,
     * unioned with the row-position and coalesce outputs of the produce set, each offset by its own {@code firstRowId}.
     * A name shared by a positional-delete predicate and a row-position output is synthesized once; the two must agree
     * on the offset.
     */
    private static List<RowPositionColumn> synthesizedColumns(
            ParquetSchema fileSchema, Predicate predicate, Projection projection) {
        Map<ColumnPath, RowPositionColumn> byName = new LinkedHashMap<>();
        for (ColumnPath column : Predicate.rowPositionColumns(predicate)) {
            validateSynthesizedName(fileSchema, column);
            byName.putIfAbsent(column, RowPositionColumn.position(column, 0L));
        }
        if (projection instanceof Projection.Of(SequencedSet<Projection.Column> columns)) {
            for (Projection.Column column : columns) {
                addSynthesizedColumn(fileSchema, byName, column);
            }
        }
        return List.copyOf(byName.values());
    }

    private static void addSynthesizedColumn(
            ParquetSchema fileSchema, Map<ColumnPath, RowPositionColumn> byName, Projection.Column column) {
        switch (column) {
            case Projection.Column.RowPosition(ColumnPath name, long firstRowId) ->
                addRowPositionOutput(fileSchema, byName, name, firstRowId);
            case Projection.Column.Coalesce(
                    ColumnPath name,
                    ColumnPath sourceColumn,
                    Projection.Column.Coalesce.Fallback fallback) ->
                byName.put(name, coalesceColumn(fileSchema, name, sourceColumn, fallback));
            default -> {
                // physical, constant, null, and promoted columns synthesize nothing
            }
        }
    }

    private static void addRowPositionOutput(
            ParquetSchema fileSchema, Map<ColumnPath, RowPositionColumn> byName, ColumnPath name, long firstRowId) {
        validateSynthesizedName(fileSchema, name);
        RowPositionColumn previous = byName.putIfAbsent(name, RowPositionColumn.position(name, firstRowId));
        if (previous != null && previous.firstRowId() != firstRowId) {
            throw new ParquetSchemaException("Conflicting first row id for the row-position column " + name.dot() + ": "
                    + previous.firstRowId() + " and " + firstRowId);
        }
    }

    /**
     * A coalesce column presented under its source name is exempt from {@link #validateSynthesizedName}: its name
     * deliberately equals a physical leaf (the materialized lineage column it coalesces with), which the row-position
     * rule forbids for a pure synthesized column. A renamed coalesce presents a new output column and its name must
     * pass the same rule.
     */
    private static RowPositionColumn coalesceColumn(
            ParquetSchema fileSchema,
            ColumnPath name,
            ColumnPath sourceColumn,
            Projection.Column.Coalesce.Fallback fallback) {
        if (!name.equals(sourceColumn)) {
            validateSynthesizedName(fileSchema, name);
        }
        return switch (fallback) {
            case Projection.Column.Coalesce.Fallback.Position(long firstRowId) ->
                RowPositionColumn.coalesceWithPosition(name, sourceColumn, firstRowId);
            case Projection.Column.Coalesce.Fallback.Constant(Value value) ->
                RowPositionColumn.coalesceWithConstant(name, sourceColumn, longValue(value));
        };
    }

    private static long longValue(Value value) {
        if (value instanceof Value.LongVal(long v)) {
            return v;
        }
        throw new ParquetSchemaException("A coalesce constant fallback must be a long value, got " + value);
    }

    /**
     * Rejects a caller-named synthesized column (a row position, or a renamed coalesce's presented name) the reader
     * cannot produce: a nested path (the synthesized column is always a top-level leaf), or a name a physical column of
     * the file already has (the produced column cannot coexist with a real column at that path). The caller must pick a
     * free top-level name (Iceberg uses {@code _pos}).
     */
    private static void validateSynthesizedName(ParquetSchema fileSchema, ColumnPath column) {
        if (column.numParts() > 1) {
            throw new ParquetSchemaException(
                    "A synthesized column must be a top-level name, not the nested path " + column.dot());
        }
        if (fileSchema.find(column).isPresent()) {
            throw new ParquetSchemaException("Cannot synthesize the " + column.dot()
                    + " column: the file already has a physical column with that name");
        }
    }

    /**
     * The columns synthesized by the read: the row positions named by the predicate, and the row-position and coalesce
     * outputs of the produce set.
     */
    List<RowPositionColumn> synthesizedColumns() {
        return synthesizedColumns;
    }

    /** Whether the read evaluates the predicate on each row: when asked to, and to synthesize a column. */
    boolean recordLevel() {
        return recordLevel;
    }

    /** The projection the filter pipeline and decode run over: physical leaves only. */
    Projection scanProjection() {
        return scanProjection;
    }

    /** The schema a surviving batch is narrowed to before the produce step: the physical output columns. */
    ParquetSchema physicalOutputSchema() {
        return physicalOutputSchema;
    }

    /** The schema the read materializes: the produced result schema (the physical output schema when no shaping). */
    ParquetSchema producedSchema() {
        return producedSchema;
    }

    /** True when producing the result needs shaping beyond the physical passthrough (rename, fill, widen, reorder). */
    boolean needsShaping() {
        return shaping.isPresent();
    }

    /** Shapes a decoded physical batch into the produce set, or returns it unchanged when no shaping is needed. */
    ParquetRecordBatch produce(ParquetRecordBatch base) {
        return shaping.map(of -> OutputBatches.produce(base, of, producedSchema))
                .orElse(base);
    }
}
