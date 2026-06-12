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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.batch.IntSequence;
import io.tileverse.parquetry.batch.LeafLevels;
import io.tileverse.parquetry.batch.Levels;
import io.tileverse.parquetry.batch.Validity;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.schema.ColumnPath;

class BatchLevelsTest {

    private static final ColumnPath LEAF = ColumnPath.of("list", "element");

    @Test
    void copiesHeapBackedLevelsReadBackIdentically() {
        int[] rep = {0, 1, 1, 0, 0, 1};
        int[] def = {3, 3, 2, 1, 3, 3};
        int numRows = countRows(rep);

        SegmentPool pool = SegmentPool.create();
        DecodeBufferAllocator allocator = TestDecodeBuffers.ample(pool);

        try (BatchLevels batchLevels = build(allocator, Levels.of(rep), Levels.of(def), numRows)) {
            LeafLevels leaf = batchLevels.leafLevels().get(LEAF);
            assertReadBack(leaf, rep, def);
        }
        assertThat(pool.stats().outstandingBorrows()).isZero();
    }

    @Test
    void copiesSegmentBackedLevelsReadBackIdentically() {
        int[] rep = {0, 1, 0, 0, 1, 1};
        int[] def = {2, 2, 1, 2, 2, 2};
        int numRows = countRows(rep);

        try (Arena arena = Arena.ofConfined()) {
            Levels repLevels = nativeOrderLevels(arena, rep);
            Levels defLevels = nativeOrderLevels(arena, def);

            SegmentPool pool = SegmentPool.create();
            DecodeBufferAllocator allocator = TestDecodeBuffers.ample(pool);

            try (BatchLevels batchLevels = build(allocator, repLevels, defLevels, numRows)) {
                LeafLevels leaf = batchLevels.leafLevels().get(LEAF);
                assertReadBack(leaf, rep, def);
            }
            assertThat(pool.stats().outstandingBorrows()).isZero();
        }
    }

    static List<Arguments> rowStartScenarios() {
        return List.of(
                Arguments.of(new int[] {0, 1, 1, 0, 0, 1}),
                Arguments.of(new int[] {0, 0, 0}),
                Arguments.of(new int[] {0, 1, 1, 1, 0}),
                Arguments.of(new int[] {0}));
    }

    @ParameterizedTest
    @MethodSource("rowStartScenarios")
    void rowStartsMatchAReferenceScan(int[] rep) {
        int[] def = new int[rep.length];
        int numRows = countRows(rep);

        SegmentPool pool = SegmentPool.create();
        DecodeBufferAllocator allocator = TestDecodeBuffers.ample(pool);

        try (BatchLevels batchLevels = build(allocator, Levels.of(rep), Levels.of(def), numRows)) {
            IntSequence rowStarts = batchLevels.leafLevels().get(LEAF).rowStarts();
            int[] expected = referenceRowStarts(rep, numRows);

            assertThat(rowStarts.size()).isEqualTo(numRows + 1);
            for (int r = 0; r <= numRows; r++) {
                assertThat(rowStarts.get(r)).as("rowStarts[%d]", r).isEqualTo(expected[r]);
            }
            assertThat(rowStarts.get(numRows))
                    .as("final entry == window length")
                    .isEqualTo(rep.length);
        }
        assertThat(pool.stats().outstandingBorrows()).isZero();
    }

    @Test
    void groupValidityMatchesAReference() {
        int[] rep = {0, 1, 0, 0, 1};
        int[] def = {3, 3, 1, 3, 3};
        int numRows = countRows(rep);
        int groupDefLevel = 2;

        SegmentPool pool = SegmentPool.create();
        DecodeBufferAllocator allocator = TestDecodeBuffers.ample(pool);

        try (BatchLevels batchLevels = build(allocator, Levels.of(rep), Levels.of(def), numRows)) {
            Validity validity = batchLevels.groupValidity(LEAF, groupDefLevel);
            Validity expected = referenceGroupValidity(rep, def, numRows, groupDefLevel);

            assertThat(validity.size()).isEqualTo(numRows);
            for (int r = 0; r < numRows; r++) {
                assertThat(validity.isValid(r)).as("row %d validity", r).isEqualTo(expected.isValid(r));
            }
        }
        assertThat(pool.stats().outstandingBorrows()).isZero();
    }

    @Test
    void groupDefLevelZeroIsAllValidWithoutScanning() {
        int[] rep = {0, 1, 0, 0};
        int[] def = {0, 0, 0, 0};
        int numRows = countRows(rep);

        SegmentPool pool = SegmentPool.create();
        DecodeBufferAllocator allocator = TestDecodeBuffers.ample(pool);

        try (BatchLevels batchLevels = build(allocator, Levels.of(rep), Levels.of(def), numRows)) {
            Validity validity = batchLevels.groupValidity(LEAF, 0);

            assertThat(validity.hasNulls()).isFalse();
            assertThat(validity.size()).isEqualTo(numRows);
        }
        assertThat(pool.stats().outstandingBorrows()).isZero();
    }

    @Test
    void buildBorrowsOneSegmentAndCloseReturnsIt() {
        int[] rep = {0, 1, 0};
        int[] def = {1, 1, 1};
        int numRows = countRows(rep);

        SegmentPool pool = SegmentPool.create();
        DecodeBufferAllocator allocator = TestDecodeBuffers.ample(pool);
        long baseline = pool.stats().outstandingBorrows();

        BatchLevels batchLevels = build(allocator, Levels.of(rep), Levels.of(def), numRows);
        assertThat(pool.stats().outstandingBorrows()).isEqualTo(baseline + 1);

        batchLevels.close();
        assertThat(pool.stats().outstandingBorrows()).isEqualTo(baseline);

        batchLevels.close();
        assertThat(pool.stats().outstandingBorrows()).isEqualTo(baseline);
    }

    @Test
    void copiesOnlyTheWindowedRegionOfALongerLevels() {
        int[] fullRep = {9, 9, 0, 1, 0, 9, 9};
        int[] fullDef = {7, 7, 2, 2, 1, 7, 7};
        int from = 2;
        int length = 3;
        int[] windowRep = {0, 1, 0};
        int[] windowDef = {2, 2, 1};
        int numRows = countRows(windowRep);

        SegmentPool pool = SegmentPool.create();
        DecodeBufferAllocator allocator = TestDecodeBuffers.ample(pool);

        LevelSlice repSlice = new LevelSlice(Levels.of(fullRep), from, length);
        LevelSlice defSlice = new LevelSlice(Levels.of(fullDef), from, length);

        try (BatchLevels batchLevels =
                BatchLevels.build(allocator, Map.of(LEAF, repSlice), Map.of(LEAF, defSlice), Set.of(LEAF), numRows)) {
            LeafLevels leaf = batchLevels.leafLevels().get(LEAF);
            assertReadBack(leaf, windowRep, windowDef);
        }
        assertThat(pool.stats().outstandingBorrows()).isZero();
    }

    @Test
    void emptyRepeatedLeavesAcquiresNothing() {
        SegmentPool pool = SegmentPool.create();
        DecodeBufferAllocator allocator = TestDecodeBuffers.ample(pool);
        long baseline = pool.stats().outstandingBorrows();

        try (BatchLevels batchLevels = BatchLevels.build(allocator, Map.of(), Map.of(), Set.of(), 4)) {
            assertThat(batchLevels.leafLevels()).isEmpty();
            assertThat(pool.stats().outstandingBorrows()).isEqualTo(baseline);
        }
        assertThat(pool.stats().outstandingBorrows()).isEqualTo(baseline);
    }

    @Test
    void missingDefSliceForARequestedLeafThrows() {
        int[] rep = {0, 1, 0};
        LevelSlice repSlice = LevelSlice.ofWhole(Levels.of(rep));

        SegmentPool pool = SegmentPool.create();
        DecodeBufferAllocator allocator = TestDecodeBuffers.ample(pool);
        long baseline = pool.stats().outstandingBorrows();

        Map<ColumnPath, LevelSlice> repByLeaf = Map.of(LEAF, repSlice);
        Map<ColumnPath, LevelSlice> noDefByLeaf = Map.of();
        Set<ColumnPath> requestedLeaves = Set.of(LEAF);
        assertThatThrownBy(() -> BatchLevels.build(allocator, repByLeaf, noDefByLeaf, requestedLeaves, 2))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(LEAF.toString());
        assertThat(pool.stats().outstandingBorrows()).isEqualTo(baseline);
    }

    @Test
    void moreRowBoundariesThanTheBatchRowsThrowsAndReleasesTheAcquisition() {
        int[] rep = {0, 0, 0, 0};
        int[] def = {1, 1, 1, 1};
        int claimedRows = 2;

        SegmentPool pool = SegmentPool.create();
        DecodeBufferAllocator allocator = TestDecodeBuffers.ample(pool);
        long baseline = pool.stats().outstandingBorrows();

        Levels repLevels = Levels.of(rep);
        Levels defLevels = Levels.of(def);
        assertThatThrownBy(() -> build(allocator, repLevels, defLevels, claimedRows))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("4")
                .hasMessageContaining("2");
        assertThat(pool.stats().outstandingBorrows())
                .as("the failure path releases the acquisition")
                .isEqualTo(baseline);
    }

    @Test
    void aRepWindowWhoseFirstEntryIsNotZeroThrowsAndReleasesTheAcquisition() {
        int[] rep = {1, 1, 0};
        int[] def = {1, 1, 1};

        SegmentPool pool = SegmentPool.create();
        DecodeBufferAllocator allocator = TestDecodeBuffers.ample(pool);
        long baseline = pool.stats().outstandingBorrows();

        Levels repLevels = Levels.of(rep);
        Levels defLevels = Levels.of(def);
        assertThatThrownBy(() -> build(allocator, repLevels, defLevels, 1)).isInstanceOf(IllegalStateException.class);
        assertThat(pool.stats().outstandingBorrows())
                .as("the failure path releases the acquisition")
                .isEqualTo(baseline);
    }

    private static BatchLevels build(DecodeBufferAllocator allocator, Levels rep, Levels def, int numRows) {
        return BatchLevels.build(
                allocator,
                Map.of(LEAF, LevelSlice.ofWhole(rep)),
                Map.of(LEAF, LevelSlice.ofWhole(def)),
                Set.of(LEAF),
                numRows);
    }

    private static void assertReadBack(LeafLevels leaf, int[] rep, int[] def) {
        assertThat(leaf.entryCount()).isEqualTo(rep.length);
        assertThat(leaf.repLevels().size()).isEqualTo(rep.length);
        assertThat(leaf.defLevels().size()).isEqualTo(def.length);
        for (int i = 0; i < rep.length; i++) {
            assertThat(leaf.repLevels().get(i)).as("rep[%d]", i).isEqualTo(rep[i]);
            assertThat(leaf.defLevels().get(i)).as("def[%d]", i).isEqualTo(def[i]);
        }
    }

    private static Levels nativeOrderLevels(Arena arena, int[] values) {
        MemorySegment segment = arena.allocate((long) values.length * Integer.BYTES);
        for (int i = 0; i < values.length; i++) {
            segment.setAtIndex(ValueLayout.JAVA_INT_UNALIGNED, i, values[i]);
        }
        return Levels.ofSegment(segment, values.length);
    }

    private static int countRows(int[] rep) {
        int rows = 0;
        for (int level : rep) {
            if (level == 0) {
                rows++;
            }
        }
        return rows;
    }

    private static int[] referenceRowStarts(int[] rep, int numRows) {
        int[] starts = new int[numRows + 1];
        int row = 0;
        for (int i = 0; i < rep.length; i++) {
            if (rep[i] == 0) {
                starts[row++] = i;
            }
        }
        starts[numRows] = rep.length;
        return starts;
    }

    private static Validity referenceGroupValidity(int[] rep, int[] def, int numRows, int groupDefLevel) {
        int[] starts = referenceRowStarts(rep, numRows);
        BitSet valid = new BitSet(numRows);
        for (int r = 0; r < numRows; r++) {
            if (def[starts[r]] >= groupDefLevel) {
                valid.set(r);
            }
        }
        return Validity.of(valid, numRows);
    }
}
