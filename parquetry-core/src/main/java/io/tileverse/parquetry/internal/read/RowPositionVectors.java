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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.ConstantVectors;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.OutputBatches;
import io.tileverse.parquetry.columnar.Selection;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * The synthesized row-position columns of one batch: the schema leaves they add, the map from a batch's logical rows to
 * their row-group-relative physical positions, and the vectors themselves.
 *
 * <p>A synthesized column reports each row's true position in the data file - the row group's file row offset plus the
 * column's own first row id plus the row's row-group-relative position - which holds however few of the row group's
 * rows a batch ends up holding. A coalesce column reads the decoded physical column at its source and fills each null
 * cell with that position or with a constant.
 *
 * <p>Both decode paths build their positions here: an unfiltered batch over a contiguous run of the row group's rows
 * ({@link #window}), and a filtered batch over the rows that survived a predicate within such a run
 * ({@link #survivors}).
 */
final class RowPositionVectors {

    private RowPositionVectors() {}

    /**
     * The top-level schema leaves {@code columns} add to a batch. A coalesce presented under its source name reuses
     * that physical leaf; a pure row position and a renamed coalesce each add a new leaf.
     */
    static List<SchemaNode.Primitive> leaves(List<RowPositionColumn> columns) {
        List<SchemaNode.Primitive> leaves = new ArrayList<>(columns.size());
        for (RowPositionColumn column : columns) {
            if (!column.reusesSourceLeaf()) {
                leaves.add(OutputBatches.rowPositionLeaf(column.name().name(), -1));
            }
        }
        return leaves;
    }

    /**
     * The row-group-relative physical position of each of the {@code rowCount} logical rows a batch holds, beginning at
     * the row group's {@code firstRowDenseIndex}-th read row. With no row mask the read rows are the row group's own
     * rows in order. With a row mask they are the surviving rows in row-group order, and the dense indexes
     * {@code [firstRowDenseIndex, firstRowDenseIndex + rowCount)} map to the matching surviving positions.
     */
    static Selection window(Optional<RowMask> rowMask, long firstRowDenseIndex, int rowCount) {
        if (rowMask.isEmpty()) {
            return Selection.range(Math.toIntExact(firstRowDenseIndex), rowCount);
        }
        return maskedWindow(rowMask.orElseThrow().survivingRows(), firstRowDenseIndex, rowCount);
    }

    /**
     * The row-group-relative physical position of each row a predicate kept within the read window
     * {@code [firstRowDenseIndex, firstRowDenseIndex + windowRows)}. {@code survivingRows} indexes rows within that
     * window, from zero; the returned selection exposes one position per set bit, in row order.
     */
    static Selection survivors(
            Optional<RowMask> rowMask, long firstRowDenseIndex, int windowRows, BitSet survivingRows) {
        Selection windowPositions = window(rowMask, firstRowDenseIndex, windowRows);
        BitSet positions = new BitSet();
        for (int row = survivingRows.nextSetBit(0);
                row >= 0 && row < windowRows;
                row = survivingRows.nextSetBit(row + 1)) {
            positions.set(windowPositions.physical(row));
        }
        return Selection.bits(positions);
    }

    /**
     * {@code vectors} plus one vector per column of {@code synthesis}, over the {@code rowCount} rows at
     * {@code positions}. One position map serves every column, each offset by the row group base plus its own first row
     * id: a within-file position (a first row id of 0) and an Iceberg row id over the same rows get distinct vectors.
     */
    static Map<ColumnPath, ColumnVector> add(
            Map<ColumnPath, ColumnVector> vectors, RowPositionSynthesis synthesis, Selection positions, int rowCount) {
        Map<ColumnPath, ColumnVector> withPosition = new HashMap<>(vectors);
        for (RowPositionColumn column : synthesis.columns()) {
            withPosition.put(column.name(), synthesizedColumn(column, synthesis.base(), positions, rowCount, vectors));
        }
        return withPosition;
    }

    /**
     * The vector for one synthesized column. A pure row position is {@code base + firstRowId} plus each row's position.
     * A coalesce reads the decoded physical column at its source and fills each null cell with the row position or the
     * constant; the physical value wins where present.
     */
    private static ColumnVector synthesizedColumn(
            RowPositionColumn column,
            long groupBase,
            Selection positions,
            int rowCount,
            Map<ColumnPath, ColumnVector> vectors) {
        if (!column.isCoalesce()) {
            return LongVector.rowPositions(groupBase + column.firstRowId(), positions, rowCount);
        }
        LongVector physical = coalesceSource(vectors, column.coalesceSource().orElseThrow());
        LongVector fallback = column.coalesceConstant().isPresent()
                ? (LongVector) ConstantVectors.of(
                        new Value.LongVal(column.coalesceConstant().getAsLong()), rowCount)
                : LongVector.rowPositions(groupBase + column.firstRowId(), positions, rowCount);
        return LongVector.coalesced(physical, fallback);
    }

    private static LongVector coalesceSource(Map<ColumnPath, ColumnVector> vectors, ColumnPath source) {
        if (vectors.get(source) instanceof LongVector longVector) {
            return longVector;
        }
        throw new IllegalStateException("coalesce source column " + source + " was not decoded as an INT64 column");
    }

    /**
     * The surviving row-group positions for the dense window {@code [firstRowDenseIndex, firstRowDenseIndex +
     * rowCount)}: it walks the surviving ranges, skips the first {@code firstRowDenseIndex} surviving rows, then sets a
     * bit per row-group-relative position for the next {@code rowCount} surviving rows.
     */
    private static Selection maskedWindow(RowRanges survivingRows, long firstRowDenseIndex, int rowCount) {
        BitSet positions = new BitSet();
        long dense = 0L;
        long windowEnd = firstRowDenseIndex + rowCount;
        for (RowRanges.Range range : survivingRows.ranges()) {
            for (long position = range.first(); position <= range.last(); position++) {
                if (dense >= firstRowDenseIndex && dense < windowEnd) {
                    positions.set(Math.toIntExact(position));
                }
                dense++;
                if (dense >= windowEnd) {
                    return Selection.bits(positions);
                }
            }
        }
        return Selection.bits(positions);
    }
}
