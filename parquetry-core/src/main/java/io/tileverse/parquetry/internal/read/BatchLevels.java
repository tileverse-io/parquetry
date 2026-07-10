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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import io.tileverse.parquetry.columnar.IntSequence;
import io.tileverse.parquetry.columnar.LeafLevels;
import io.tileverse.parquetry.columnar.Levels;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.format.ParquetLayouts;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Batch-owned copies of the per-leaf level windows for the repeated leaves of one batch. The reader exposes each leaf's
 * window as a {@link LevelSlice} over per-reader scratch memory that the next page decode overwrites; a batch that
 * outlives that decode must own a copy. {@link #build} copies every repeated leaf's rep and def windows, plus a
 * row-boundary index, into one pooled off-heap segment acquired through the decode-buffer valve and released on
 * {@link #close()}.
 *
 * <p>Within the segment each leaf occupies three consecutive windows: the rep levels (native-order ints, the order
 * {@link Levels#ofSegment} reads), the def levels (same), and the row starts ({@link ParquetLayouts#INT32}
 * little-endian, the order {@link IntSequence#ofSegment} reads).
 */
final class BatchLevels implements AutoCloseable {

    private final SegmentPool.Pooled pooled;
    private final Map<ColumnPath, LeafLevels> leafLevels;
    private final int numRows;
    private boolean closed;

    private BatchLevels(SegmentPool.Pooled pooled, Map<ColumnPath, LeafLevels> leafLevels, int numRows) {
        this.pooled = pooled;
        this.leafLevels = Collections.unmodifiableMap(leafLevels);
        this.numRows = numRows;
    }

    /**
     * Copies every repeated leaf's rep/def windows and row-boundary index into one pooled segment. Each repeated leaf
     * must have both a rep and a def slice; a missing slice is an {@link IllegalStateException}. When no leaf is
     * repeated this acquires no segment and returns an empty instance whose {@link #close()} is a no-op.
     */
    static BatchLevels build(
            DecodeBufferAllocator allocator,
            Map<ColumnPath, LevelSlice> repLevelsByLeaf,
            Map<ColumnPath, LevelSlice> defLevelsByLeaf,
            Set<ColumnPath> repeatedLeaves,
            int numRows) {

        if (repeatedLeaves.isEmpty()) {
            return new BatchLevels(null, Map.of(), numRows);
        }

        long totalBytes = totalSegmentBytes(repLevelsByLeaf, defLevelsByLeaf, repeatedLeaves, numRows);
        SegmentPool.Pooled pooled = allocator.acquireMandatory(totalBytes);
        try {
            Map<ColumnPath, LeafLevels> leafLevels =
                    copyLeaves(pooled.segment(), repLevelsByLeaf, defLevelsByLeaf, repeatedLeaves, numRows);
            return new BatchLevels(pooled, leafLevels, numRows);
        } catch (RuntimeException e) {
            pooled.close();
            throw e;
        }
    }

    private static long totalSegmentBytes(
            Map<ColumnPath, LevelSlice> repLevelsByLeaf,
            Map<ColumnPath, LevelSlice> defLevelsByLeaf,
            Set<ColumnPath> repeatedLeaves,
            int numRows) {
        long ints = 0;
        for (ColumnPath leaf : repeatedLeaves) {
            LevelSlice rep = requireSlice(repLevelsByLeaf, leaf, "repetition");
            LevelSlice def = requireSlice(defLevelsByLeaf, leaf, "definition");
            ints += rep.length() + def.length() + (numRows + 1L);
        }
        return ints * Integer.BYTES;
    }

    private static Map<ColumnPath, LeafLevels> copyLeaves(
            MemorySegment segment,
            Map<ColumnPath, LevelSlice> repLevelsByLeaf,
            Map<ColumnPath, LevelSlice> defLevelsByLeaf,
            Set<ColumnPath> repeatedLeaves,
            int numRows) {

        Map<ColumnPath, LeafLevels> leafLevels = new LinkedHashMap<>();
        long offsetInts = 0;
        for (ColumnPath leaf : repeatedLeaves) {
            LevelSlice rep = requireSlice(repLevelsByLeaf, leaf, "repetition");
            LevelSlice def = requireSlice(defLevelsByLeaf, leaf, "definition");
            offsetInts = copyLeaf(segment, offsetInts, leaf, rep, def, numRows, leafLevels);
        }
        return leafLevels;
    }

    private static long copyLeaf(
            MemorySegment segment,
            long offsetInts,
            ColumnPath leaf,
            LevelSlice rep,
            LevelSlice def,
            int numRows,
            Map<ColumnPath, LeafLevels> leafLevels) {

        long repOffsetInts = offsetInts;
        Levels repWindow = copyLevelsWindow(segment, repOffsetInts, rep);

        long defOffsetInts = repOffsetInts + rep.length();
        Levels defWindow = copyLevelsWindow(segment, defOffsetInts, def);

        long rowStartsOffsetInts = defOffsetInts + def.length();
        IntSequence rowStarts = buildRowStarts(segment, rowStartsOffsetInts, rep, numRows);

        leafLevels.put(leaf, new LeafLevels(repWindow, defWindow, rowStarts));
        return rowStartsOffsetInts + numRows + 1L;
    }

    private static Levels copyLevelsWindow(MemorySegment segment, long offsetInts, LevelSlice slice) {
        long byteOffset = offsetInts * Integer.BYTES;
        for (int i = 0; i < slice.length(); i++) {
            segment.setAtIndex(ValueLayout.JAVA_INT_UNALIGNED, offsetInts + i, slice.at(i));
        }
        MemorySegment window = segment.asSlice(byteOffset, (long) slice.length() * Integer.BYTES);
        return Levels.ofSegment(window, slice.length());
    }

    private static IntSequence buildRowStarts(MemorySegment segment, long offsetInts, LevelSlice rep, int numRows) {
        int boundaries = 0;
        for (int i = 0; i < rep.length(); i++) {
            boolean rowBoundary = rep.at(i) == 0;
            if (rowBoundary) {
                // A malformed stream can hold more boundaries than the batch's rows; keep counting for the
                // mismatch check below, but never write past the row-starts window.
                if (boundaries < numRows) {
                    writeRowStart(segment, offsetInts + boundaries, i);
                }
                boundaries++;
            }
        }
        requireExpectedBoundaries(boundaries, numRows, rep);
        writeRowStart(segment, offsetInts + numRows, rep.length());

        long byteOffset = offsetInts * Integer.BYTES;
        MemorySegment window = segment.asSlice(byteOffset, (numRows + 1L) * Integer.BYTES);
        return IntSequence.ofSegment(window, numRows + 1);
    }

    private static void writeRowStart(MemorySegment segment, long indexInts, int position) {
        segment.setAtIndex(ParquetLayouts.INT32, indexInts, position);
    }

    private static void requireExpectedBoundaries(int boundaries, int numRows, LevelSlice rep) {
        if (boundaries != numRows) {
            throw new IllegalStateException(
                    "rep window holds " + boundaries + " row boundaries but the batch expects " + numRows);
        }
        boolean nonEmptyWithoutLeadingBoundary = rep.length() > 0 && rep.at(0) != 0;
        if (nonEmptyWithoutLeadingBoundary) {
            throw new IllegalStateException("a non-empty rep window must start with a row boundary (level 0)");
        }
    }

    private static LevelSlice requireSlice(Map<ColumnPath, LevelSlice> byLeaf, ColumnPath leaf, String stream) {
        LevelSlice slice = byLeaf.get(leaf);
        if (slice == null) {
            throw new IllegalStateException("repeated leaf " + leaf + " has no " + stream + " level window");
        }
        return slice;
    }

    Map<ColumnPath, LeafLevels> leafLevels() {
        return leafLevels;
    }

    /**
     * The per-row validity of the nested group whose presence is recorded at {@code groupDefLevel}: row {@code r} is
     * valid when the def level at the row's first entry is at least {@code groupDefLevel}. A group defined at level
     * zero is always present, returning the bitmap-free all-valid mask.
     */
    Validity groupValidity(ColumnPath structureLeaf, int groupDefLevel) {
        if (groupDefLevel == 0) {
            return Validity.allValid(numRows);
        }
        LeafLevels leaf = leafLevels.get(structureLeaf);
        if (leaf == null) {
            throw new IllegalStateException("no batch-owned levels for structure leaf " + structureLeaf);
        }
        Levels defLevels = leaf.defLevels();
        IntSequence rowStarts = leaf.rowStarts();
        BitSet valid = new BitSet(numRows);
        for (int row = 0; row < numRows; row++) {
            boolean present = defLevels.get(rowStarts.get(row)) >= groupDefLevel;
            if (present) {
                valid.set(row);
            }
        }
        return Validity.of(valid, numRows);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (pooled != null) {
            pooled.close();
        }
    }
}
