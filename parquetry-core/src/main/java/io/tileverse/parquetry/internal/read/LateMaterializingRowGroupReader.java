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
package io.tileverse.parquetry.internal.read;

import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.batch.VectorizedPredicateEvaluator;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

/**
 * Two-phase late-materializing reader for one row group whose scanned columns are all flat.
 *
 * <p>The classic decode path materializes every output column for every surviving row, then drops the rows that fail
 * the predicate. Late materialization splits decoding into two passes to avoid decoding output values for rows the
 * predicate rejects:
 *
 * <ol>
 *   <li><b>Phase 1</b> decodes only the predicate columns over the surviving rows (the page-skip mask, or all rows when
 *       there is no mask), evaluates the predicate per row, and records the matching rows as a {@link Selection}.
 *   <li><b>Phase 2</b> decodes the output columns under that {@link Selection} with skip-decode on, materializing
 *       values only for the matching rows.
 * </ol>
 *
 * <p>Columns that appear in both the predicate and the output are simply decoded again in phase 2 under skip-decode;
 * because skip-decode touches only the selected rows, the repeated work is small and needs no special handling.
 *
 * <p>This reader applies to flat scanned columns only. The caller guarantees that every scan leaf is flat and that an
 * {@link OffsetIndex} is available for each scan leaf (skip-decode maps the selection's row spans to pages through the
 * offset index).
 */
public final class LateMaterializingRowGroupReader {

    private final List<FetchedColumnChunk> chunks;
    private final ParquetSchema fileSchema;
    private final ParquetSchema outputSchema;
    private final Set<ColumnPath> predicateLeaves;
    private final Predicate predicate;
    private final OptionalInt batchSizeCap;
    private final Optional<RowMask> rowMask;
    private final Map<ColumnPath, OffsetIndex> offsetIndexes;
    private final long numRows;
    private final BatchForm outputForm;
    private final DecodeBufferAllocator decodeBufferAllocator;

    // Phase-1 predicate-column page tally, captured before the phase-1 reader is closed. The closed reader can no
    // longer
    // report it, yet the row group's read event must account for the predicate-column pages too.
    private BatchRowGroupReader.PageCounts phase1PageCounts = BatchRowGroupReader.PageCounts.ZERO;

    // Phase-1 decoded-row tally, captured alongside the page tally for the same reason.
    private long phase1RowsProduced;

    // S107: aggregates the late-materialization decode inputs; a parameter object would only relocate the arity.
    @SuppressWarnings("java:S107")
    public LateMaterializingRowGroupReader(
            @NonNull DecodeBufferAllocator decodeBufferAllocator,
            @NonNull List<FetchedColumnChunk> chunks,
            @NonNull ParquetSchema fileSchema,
            @NonNull ParquetSchema outputSchema,
            @NonNull Set<ColumnPath> predicateLeaves,
            @NonNull Predicate predicate,
            @NonNull OptionalInt batchSizeCap,
            @NonNull Optional<RowMask> rowMask,
            @NonNull Map<ColumnPath, OffsetIndex> offsetIndexes,
            long numRows,
            @NonNull BatchForm outputForm) {
        this.decodeBufferAllocator = decodeBufferAllocator;
        this.chunks = List.copyOf(chunks);
        this.fileSchema = fileSchema;
        this.outputSchema = outputSchema;
        this.predicateLeaves = Set.copyOf(predicateLeaves);
        this.predicate = predicate;
        this.batchSizeCap = batchSizeCap;
        this.rowMask = rowMask;
        this.offsetIndexes = Map.copyOf(offsetIndexes);
        this.numRows = numRows;
        this.outputForm = outputForm;
    }

    /**
     * Phase 1: decodes the predicate columns over the surviving rows, evaluates the predicate per row, and returns the
     * matching rows as a {@link Selection}. An empty selection means no surviving row satisfied the predicate.
     */
    public Selection selectMatching() {
        RowRanges surviving = rowMask.map(RowMask::survivingRows).orElseGet(() -> RowRanges.all(numRows));
        return phase1Selection(surviving);
    }

    /**
     * Phase 2: returns a reader that, under {@code selection}, decodes the output columns with skip-decode on,
     * materializing values only for the matching rows. The caller drains and closes the reader. Returns a reader over
     * an empty selection only when the caller passes one; callers should check {@link Selection#isEmpty()} first.
     */
    public BatchRowGroupReader outputReader(Selection selection) {
        List<ColumnPath> outputLeaves = outputSchema.leafColumns();
        List<FetchedColumnChunk> outputChunks = chunks.stream()
                .filter(chunk -> outputLeaves.contains(chunk.path()))
                .toList();
        RowMask selectionMask = new RowMask(selection.rows(), outputOffsetIndexes(outputLeaves));
        return new BatchRowGroupReader(
                decodeBufferAllocator,
                outputChunks,
                outputSchema,
                fileSchema,
                batchSizeCap,
                Optional.of(selectionMask),
                true,
                outputForm);
    }

    /**
     * Phase 1: decodes the predicate columns over {@code surviving}, evaluates the predicate per row, and accumulates
     * the matching rows into a {@link Selection}. Walks the surviving absolute rows in ascending order in lockstep with
     * the phase-1 batch rows; each batch is closed as soon as its rows are evaluated.
     */
    private Selection phase1Selection(RowRanges surviving) {
        ParquetSchema predicateSchema = fileSchema.project(predicateLeaves);
        List<FetchedColumnChunk> predicateChunks = chunks.stream()
                .filter(chunk -> predicateLeaves.contains(chunk.path()))
                .toList();
        Selection.Builder selectionBuilder = Selection.builder();
        RowRangeCursor cursor = new RowRangeCursor(surviving);
        // Phase 1 batches only feed the predicate evaluator; they never reach the consumer record stream, hence the
        // form does not matter and they stay assembled (the scanned predicate columns are flat by contract anyway).
        try (BatchRowGroupReader phase1 = new BatchRowGroupReader(
                decodeBufferAllocator,
                predicateChunks,
                predicateSchema,
                fileSchema,
                batchSizeCap,
                rowMask,
                BatchForm.ASSEMBLED)) {
            while (phase1.hasMore()) {
                try (ParquetRecordBatch batch = phase1.nextBatch()) {
                    evaluateBatch(batch, cursor, selectionBuilder);
                }
            }
            phase1PageCounts = phase1.pageCounts();
            phase1RowsProduced = phase1.rowsProduced();
        }
        return selectionBuilder.build();
    }

    /**
     * The data pages phase 1 decoded and skipped over the predicate columns. Valid after {@link #selectMatching()} has
     * run; the phase-1 reader has been closed by then, hence the count is read from this retained tally rather than
     * from the reader.
     */
    public BatchRowGroupReader.PageCounts phase1PageCounts() {
        return phase1PageCounts;
    }

    /**
     * The rows phase 1 ran through decode over the predicate columns: every surviving row, whether or not it matched.
     * Valid after {@link #selectMatching()} has run, read from the retained tally like {@link #phase1PageCounts()}.
     */
    public long phase1RowsProduced() {
        return phase1RowsProduced;
    }

    private void evaluateBatch(ParquetRecordBatch batch, RowRangeCursor cursor, Selection.Builder selectionBuilder) {
        BitSet matches = VectorizedPredicateEvaluator.eval(predicate, batch);
        for (int row = 0; row < batch.rowCount(); row++) {
            long absoluteRow = cursor.next();
            selectionBuilder.accept(absoluteRow, matches.get(row));
        }
    }

    private Map<ColumnPath, OffsetIndex> outputOffsetIndexes(List<ColumnPath> outputLeaves) {
        Map<ColumnPath, OffsetIndex> result = LinkedHashMap.newLinkedHashMap(outputLeaves.size());
        for (ColumnPath leaf : outputLeaves) {
            result.put(leaf, offsetIndexes.get(leaf));
        }
        return result;
    }

    /**
     * Yields the surviving absolute row indices in ascending order, one per {@link #next()} call. Phase 1 advances this
     * cursor in lockstep with the batch rows it reads, mapping each batch row back to its absolute index within the row
     * group.
     */
    private static final class RowRangeCursor {

        private final List<RowRanges.Range> ranges;
        private int rangeIndex;
        private long nextRow;

        RowRangeCursor(RowRanges rows) {
            this.ranges = rows.ranges();
            this.rangeIndex = 0;
            this.nextRow = ranges.isEmpty() ? -1L : ranges.get(0).first();
        }

        long next() {
            if (rangeIndex >= ranges.size()) {
                throw new IllegalStateException("No more surviving rows to map");
            }
            long current = nextRow;
            RowRanges.Range range = ranges.get(rangeIndex);
            if (current < range.last()) {
                nextRow = current + 1;
            } else {
                rangeIndex++;
                nextRow = rangeIndex < ranges.size() ? ranges.get(rangeIndex).first() : -1L;
            }
            return current;
        }
    }
}
