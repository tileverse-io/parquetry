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
package io.tileverse.parquetry.columnar;

import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.Map;

import io.tileverse.parquetry.record.DefaultParquetRecord;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.record.RowColumns;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

/**
 * A zero-copy view over a decoded batch that narrows it to the output columns and exposes only the rows a predicate
 * kept. It holds the decoded {@code source}, the subset of its columns that resolve in {@code outputSchema}
 * (unselected, shared with the source), and a {@link Selection} of surviving rows. It copies no column data, allocates
 * no per-row index array, and - unlike rebuilding a batch - allocates no {@link java.lang.foreign.Arena}: the source
 * owns the off-heap buffers and this view borrows them. The selection is the {@code BitSet} from predicate evaluation,
 * reused directly. Narrowing (which columns) and selection (which rows) compose on this one view.
 *
 * <p>The two consumer paths read the same survivors two ways. {@link #materialize(int)} builds a record over the
 * unselected narrowed columns at the physical row ({@code selection.physical(j)}), which lets the per-row record
 * materializer read the bulk binary backings a selected vector defers to the export boundary. {@link #columns()}
 * returns those columns each {@link ColumnVector#select(Selection) selected}, the same-type views a columnar consumer
 * reads at logical rows. The per-row machinery ({@code RowColumns}) and the selected column map are built lazily: a
 * purely columnar consumer never pays for row materialization and a purely row consumer never builds the selected map.
 *
 * <p>Bounded pinning: the view owns {@code source}; the full decoded batch stays alive until this view closes. In the
 * streaming one-live-batch model that is the same footprint the pushdown path already holds.
 */
public final class FilteredRecordBatch implements ParquetRecordBatch {

    private final ParquetRecordBatch source;
    private final ParquetSchema outputSchema;
    private final Map<ColumnPath, ColumnVector> baseColumns; // narrowed, unselected, shared with source
    private final Selection selection;

    private Map<ColumnPath, ColumnVector> selectedColumns; // lazy: only a columnar consumer needs it
    private RowColumns rowColumns; // lazy: only a row consumer needs it

    private FilteredRecordBatch(
            ParquetRecordBatch source,
            ParquetSchema outputSchema,
            Map<ColumnPath, ColumnVector> baseColumns,
            Selection selection) {
        this.source = source;
        this.outputSchema = outputSchema;
        this.baseColumns = baseColumns;
        this.selection = selection;
    }

    /**
     * A view of {@code source} narrowed to {@code outputSchema} and exposing the rows of {@code selection}. Returns
     * {@code source} unchanged when nothing is dropped: no column falls outside the output and {@code selection} is
     * {@link Selection#ALL}.
     */
    public static ParquetRecordBatch of(
            @NonNull ParquetRecordBatch source, @NonNull Selection selection, @NonNull ParquetSchema outputSchema) {
        Map<ColumnPath, ColumnVector> baseColumns = narrow(source, outputSchema);
        if (selection == Selection.ALL && baseColumns.size() == source.columns().size()) {
            return source;
        }
        return new FilteredRecordBatch(source, outputSchema, baseColumns, selection);
    }

    /**
     * A view of {@code source} narrowed to {@code outputSchema}, keeping every row. Returns {@code source} unchanged
     * when it already holds exactly the output columns. The {@code BitSet}-free entry for the read pipeline, which lets
     * the {@code internal.read} package avoid naming {@link Selection} (it has an unrelated same-named type).
     */
    public static ParquetRecordBatch narrowed(@NonNull ParquetRecordBatch source, @NonNull ParquetSchema outputSchema) {
        return of(source, Selection.ALL, outputSchema);
    }

    /**
     * A view of {@code source} narrowed to {@code outputSchema} and exposing the rows set in {@code survivors} (the
     * predicate-evaluation bit set, reused directly as a {@link Selection.Bits}). The read pipeline holds survivors as
     * a {@link BitSet} and need not name {@link Selection}.
     */
    public static ParquetRecordBatch filtered(
            @NonNull ParquetRecordBatch source, @NonNull BitSet survivors, @NonNull ParquetSchema outputSchema) {
        return of(source, Selection.bits(survivors), outputSchema);
    }

    /**
     * The source columns whose path resolves in {@code outputSchema}. Iterating the source's own column keys keeps them
     * keyed exactly as the batch holds them (a LIST or MAP column at its group path, a flat or struct field at its leaf
     * path); a predicate-only column absent from the output resolves to empty and is dropped.
     */
    private static Map<ColumnPath, ColumnVector> narrow(ParquetRecordBatch source, ParquetSchema outputSchema) {
        Map<ColumnPath, ColumnVector> kept =
                LinkedHashMap.newLinkedHashMap(source.columns().size());
        for (Map.Entry<ColumnPath, ColumnVector> column : source.columns().entrySet()) {
            if (outputSchema.find(column.getKey()).isPresent()) {
                kept.put(column.getKey(), column.getValue());
            }
        }
        return kept;
    }

    @Override
    public ParquetSchema projectedSchema() {
        return outputSchema;
    }

    @Override
    public int rowCount() {
        return selection == Selection.ALL ? source.rowCount() : selection.length();
    }

    @Override
    public Map<ColumnPath, ColumnVector> columns() {
        if (selection == Selection.ALL) {
            return baseColumns;
        }
        if (selectedColumns == null) {
            Map<ColumnPath, ColumnVector> selected = LinkedHashMap.newLinkedHashMap(baseColumns.size());
            for (Map.Entry<ColumnPath, ColumnVector> column : baseColumns.entrySet()) {
                selected.put(column.getKey(), column.getValue().select(selection));
            }
            selectedColumns = selected;
        }
        return selectedColumns;
    }

    @Override
    public ParquetRecord materialize(int rowIndex) {
        int rowCount = rowCount();
        if (rowIndex < 0 || rowIndex >= rowCount) {
            throw new IndexOutOfBoundsException("rowIndex " + rowIndex + " out of bounds [0, " + rowCount + ")");
        }
        int physical = selection == Selection.ALL ? rowIndex : selection.physical(rowIndex);
        return new DefaultParquetRecord(rowColumns(), physical);
    }

    private RowColumns rowColumns() {
        if (rowColumns == null) {
            rowColumns = RowColumns.of(outputSchema, outputSchema.root(), baseColumns);
        }
        return rowColumns;
    }

    /**
     * A dense form of this view: every column rebuilt to hold exactly the selected rows, in selection order. The
     * identity-selection case ({@code selection == ALL}, a projection-only narrowing) returns {@code this} - its
     * columns are the unselected base vectors and already read dense. Otherwise the survivor indices are materialized
     * once and every base column is gathered through {@link Compaction}, producing a {@link DefaultParquetRecordBatch}.
     *
     * <p>A shredded Variant column (top-level or nested in a struct) is unshredded first, the same form the Arrow
     * export preparation produces for a dense shredded column; the gather then works on the resulting
     * {@link VariantVector}.
     *
     * <p>Contract: the returned batch may alias this view's source memory (gathered binary values are windows into the
     * source backing) - consume it while the source batch is open. A result other than {@code this} owns a token arena
     * and must be closed; closing it closes neither this view nor its source.
     */
    public ParquetRecordBatch compacted() {
        if (selection == Selection.ALL) {
            return this;
        }
        int[] keptIndices = keptIndices();
        Map<ColumnPath, ColumnVector> dense = LinkedHashMap.newLinkedHashMap(baseColumns.size());
        for (Map.Entry<ColumnPath, ColumnVector> column : baseColumns.entrySet()) {
            ColumnVector unshredded = unshredForGather(column.getValue());
            dense.put(column.getKey(), Compaction.compact(unshredded, keptIndices));
        }
        return DefaultParquetRecordBatch.ofHeap(outputSchema, dense, keptIndices.length);
    }

    /** The physical row index of every selected row, in logical order. */
    private int[] keptIndices() {
        int length = selection.length();
        int[] kept = new int[length];
        for (int logical = 0; logical < length; logical++) {
            kept[logical] = selection.physical(logical);
        }
        return kept;
    }

    /**
     * Rewrites a shredded Variant to its unshredded form ahead of the gather: {@link Compaction} deliberately rejects
     * the shredded vector (in assembly, reaching one means the unsupported shredded-under-list/map shape), while a
     * top-level or struct-nested shredded column is a legal batch column here. Struct children are rewritten
     * recursively; every other vector passes through untouched.
     */
    private static ColumnVector unshredForGather(ColumnVector vector) {
        return switch (vector) {
            case ShreddedVariantVector shredded -> shredded.toUnshredded();
            case StructVector struct -> unshredStructChildren(struct);
            default -> vector;
        };
    }

    private static ColumnVector unshredStructChildren(StructVector struct) {
        Map<ColumnPath, ColumnVector> rewritten = null;
        for (Map.Entry<ColumnPath, ColumnVector> child : struct.children().entrySet()) {
            ColumnVector unshredded = unshredForGather(child.getValue());
            if (unshredded != child.getValue() && rewritten == null) {
                rewritten = new LinkedHashMap<>(struct.children());
            }
            if (rewritten != null) {
                rewritten.put(child.getKey(), unshredded);
            }
        }
        if (rewritten == null) {
            return struct;
        }
        return new StructVector(rewritten, struct.validity(), struct.size());
    }

    @Override
    public ParquetRecordBatch slice(int from, int count) {
        Selection windowed = selection == Selection.ALL ? Selection.range(from, count) : selection.sub(from, count);
        return new FilteredRecordBatch(source, outputSchema, baseColumns, windowed);
    }

    @Override
    public long approximateHeapBytes() {
        return source.approximateHeapBytes();
    }

    @Override
    public void attachReleaseAction(Runnable releaseAction) {
        source.attachReleaseAction(releaseAction);
    }

    @Override
    public void registerBuffer(AutoCloseable buffer) {
        source.registerBuffer(buffer);
    }

    @Override
    public void close() {
        source.close();
    }
}
