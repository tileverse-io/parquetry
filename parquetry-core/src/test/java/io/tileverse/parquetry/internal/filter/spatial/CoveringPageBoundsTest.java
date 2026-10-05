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
package io.tileverse.parquetry.internal.filter.spatial;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.format.BoundaryOrder;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.internal.filter.FilterPipeline.ColumnPageStats;
import io.tileverse.parquetry.internal.filter.spatial.CoveringPageBounds.PageUnit;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Pins how the covering leaves' column indexes become page units: the xmin leaf's pages give the spans and the four
 * leaves' per-page bounds give each span's box. Indexes are built by hand; a FLOAT value is its 4 little-endian bytes
 * as Parquet writes statistics.
 */
class CoveringPageBoundsTest {

    private static final long ROWS = 16L;

    @Test
    void alignedLeavesGiveOneUnitPerPageWithItsOwnBox() {
        ColumnPageStats xmin =
                floatLeaf(rows(0, 4, 8, 12), mins(0f, 4f, 8f, 12f), maxs(3f, 7f, 11f, 15f), nulls(0, 0, 0, 0));
        ColumnPageStats xmax =
                floatLeaf(rows(0, 4, 8, 12), mins(1f, 5f, 9f, 13f), maxs(4f, 8f, 12f, 16f), nulls(0, 0, 0, 0));
        ColumnPageStats ymin =
                floatLeaf(rows(0, 4, 8, 12), mins(10f, 14f, 18f, 22f), maxs(13f, 17f, 21f, 25f), nulls(0, 0, 0, 0));
        ColumnPageStats ymax =
                floatLeaf(rows(0, 4, 8, 12), mins(11f, 15f, 19f, 23f), maxs(14f, 18f, 22f, 26f), nulls(0, 0, 0, 0));

        List<PageUnit> units = CoveringPageBounds.unitsOf(xmin, xmax, ymin, ymax, ROWS);

        assertThat(units).hasSize(4);
        assertThat(units.get(1).firstRow()).isEqualTo(4L);
        assertThat(units.get(1).lastRow()).isEqualTo(7L);
        assertThat(units.get(1).box()).contains(Bbox.of2d(4, 14, 8, 18));
        assertThat(units.get(3).lastRow())
                .as("the last page ends at the row group's last row")
                .isEqualTo(15L);
    }

    @Test
    void aMisalignedLeafContributesEveryPageOverlappingTheSpan() {
        ColumnPageStats xmin = floatLeaf(rows(0, 8), mins(0f, 8f), maxs(7f, 15f), nulls(0, 0));
        ColumnPageStats xmax = floatLeaf(rows(0, 6, 12), mins(1f, 7f, 13f), maxs(6f, 12f, 16f), nulls(0, 0, 0));
        ColumnPageStats ymin = floatLeaf(rows(0, 8), mins(0f, 0f), maxs(0f, 0f), nulls(0, 0));
        ColumnPageStats ymax = floatLeaf(rows(0, 8), mins(1f, 1f), maxs(1f, 1f), nulls(0, 0));

        List<PageUnit> units = CoveringPageBounds.unitsOf(xmin, xmax, ymin, ymax, ROWS);

        assertThat(units).hasSize(2);
        assertThat(units.get(0).box())
                .as("rows 0-7 overlap xmax pages [0,5] and [6,11]: maxX is the larger of their maxima")
                .contains(Bbox.of2d(0, 0, 12, 1));
        assertThat(units.get(1).box()).contains(Bbox.of2d(8, 0, 16, 1));
    }

    /**
     * Four spans against a leaf whose page boundaries fall between them, which pins the fold's resumption point: page
     * [6, 9] of the {@code xmax} leaf overlaps the spans [4, 7] and [8, 11] and must be read for both, while page [0,
     * 5] must no longer be read for [8, 11] and neither of them for [12, 15].
     */
    @Test
    void aLeafPageOverlappingTwoSpansContributesToBoth() {
        ColumnPageStats xmin =
                floatLeaf(rows(0, 4, 8, 12), mins(0f, 4f, 8f, 12f), maxs(3f, 7f, 11f, 15f), nulls(0, 0, 0, 0));
        ColumnPageStats xmax = floatLeaf(rows(0, 6, 10), mins(1f, 1f, 1f), maxs(5f, 30f, 20f), nulls(0, 0, 0));
        ColumnPageStats ymin = floatLeaf(rows(0, 8), mins(0f, 0f), maxs(0f, 0f), nulls(0, 0));
        ColumnPageStats ymax = floatLeaf(rows(0, 8), mins(1f, 1f), maxs(1f, 1f), nulls(0, 0));

        List<PageUnit> units = CoveringPageBounds.unitsOf(xmin, xmax, ymin, ymax, ROWS);

        assertThat(units).hasSize(4);
        assertThat(units.get(0).box()).contains(Bbox.of2d(0, 0, 5, 1));
        assertThat(units.get(1).box())
                .as("rows 4-7 overlap the xmax pages [0,5] and [6,9]")
                .contains(Bbox.of2d(4, 0, 30, 1));
        assertThat(units.get(2).box())
                .as("rows 8-11 overlap the xmax pages [6,9] and [10,15], and no longer [0,5]")
                .contains(Bbox.of2d(8, 0, 30, 1));
        assertThat(units.get(3).box())
                .as("rows 12-15 overlap the xmax page [10,15] alone")
                .contains(Bbox.of2d(12, 0, 20, 1));
    }

    @Test
    void aNullPageInAnyOverlappingLeafLeavesTheUnitWithoutABox() {
        ColumnPageStats xmin = floatLeaf(rows(0, 8), mins(0f, 8f), maxs(7f, 15f), nulls(0, 0));
        ColumnPageStats xmax = floatLeafWithNullPage(rows(0, 8), 1);
        ColumnPageStats ymin = floatLeaf(rows(0, 8), mins(0f, 0f), maxs(0f, 0f), nulls(0, 0));
        ColumnPageStats ymax = floatLeaf(rows(0, 8), mins(1f, 1f), maxs(1f, 1f), nulls(0, 0));

        List<PageUnit> units = CoveringPageBounds.unitsOf(xmin, xmax, ymin, ymax, ROWS);

        assertThat(units.get(0).box()).isPresent();
        assertThat(units.get(1).box()).isEmpty();
    }

    @Test
    void aPageWithNullsAndABoxIsAUnitWithThatBox() {
        ColumnPageStats xmin = floatLeaf(rows(0, 8), mins(0f, 8f), maxs(7f, 15f), nulls(0, 2));
        ColumnPageStats xmax = floatLeaf(rows(0, 8), mins(1f, 9f), maxs(8f, 16f), nulls(0, 2));
        ColumnPageStats ymin = floatLeaf(rows(0, 8), mins(0f, 0f), maxs(0f, 0f), nulls(0, 2));
        ColumnPageStats ymax = floatLeaf(rows(0, 8), mins(1f, 1f), maxs(1f, 1f), nulls(0, 2));

        List<PageUnit> units = CoveringPageBounds.unitsOf(xmin, xmax, ymin, ymax, ROWS);

        assertThat(units.get(1).box())
                .as("statistics are computed over the non-null values, hence a box proves one exists")
                .contains(Bbox.of2d(8, 0, 16, 1));
    }

    @Test
    void aNaNPageStatisticLeavesTheUnitWithoutABox() {
        ColumnPageStats xmin = floatLeaf(rows(0, 8), mins(0f, 8f), maxs(7f, 15f), nulls(0, 0));
        ColumnPageStats xmax = floatLeaf(rows(0, 8), mins(1f, 9f), maxs(6f, Float.NaN), nulls(0, 0));
        ColumnPageStats ymin = floatLeaf(rows(0, 8), mins(0f, 0f), maxs(0f, 0f), nulls(0, 0));
        ColumnPageStats ymax = floatLeaf(rows(0, 8), mins(1f, 1f), maxs(1f, 1f), nulls(0, 0));

        List<PageUnit> units = CoveringPageBounds.unitsOf(xmin, xmax, ymin, ymax, ROWS);

        assertThat(units.get(0).box()).contains(Bbox.of2d(0, 0, 6, 1));
        assertThat(units.get(1).box())
                .as("a NaN bound encloses nothing, and a box that encloses nothing is no box at all")
                .isEmpty();
    }

    @Test
    void aSpanWhoseMaximumFallsBelowItsMinimumLeavesTheUnitWithoutABox() {
        ColumnPageStats xmin = floatLeaf(rows(0, 8), mins(0f, 8f), maxs(7f, 15f), nulls(0, 0));
        ColumnPageStats xmax = floatLeaf(rows(0, 8), mins(1f, 1f), maxs(6f, 2f), nulls(0, 0));
        ColumnPageStats ymin = floatLeaf(rows(0, 8), mins(0f, 0f), maxs(0f, 0f), nulls(0, 0));
        ColumnPageStats ymax = floatLeaf(rows(0, 8), mins(1f, 1f), maxs(1f, 1f), nulls(0, 0));

        List<PageUnit> units = CoveringPageBounds.unitsOf(xmin, xmax, ymin, ymax, ROWS);

        assertThat(units.get(0).box()).contains(Bbox.of2d(0, 0, 6, 1));
        assertThat(units.get(1).box())
                .as("rows 8-15 have minX 8 and maxX 2: the covering contradicts itself")
                .isEmpty();
    }

    @Test
    void anIndexWhosePageCountsDisagreeYieldsNoUnits() {
        ColumnPageStats xmin = new ColumnPageStats(
                PrimitiveKind.FLOAT,
                new ColumnIndex(
                        List.of(false),
                        List.of(bytes(0f)),
                        List.of(bytes(1f)),
                        BoundaryOrder.UNORDERED,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                new OffsetIndex(List.of(new PageLocation(0, 10, 0), new PageLocation(10, 10, 8)), Optional.empty()));
        ColumnPageStats other = floatLeaf(rows(0, 8), mins(0f, 0f), maxs(1f, 1f), nulls(0, 0));

        assertThat(CoveringPageBounds.unitsOf(xmin, other, other, other, ROWS)).isEmpty();
    }

    // --- fixture builders ---

    private static ColumnPageStats floatLeaf(long[] firstRows, float[] mins, float[] maxs, long[] nullCounts) {
        List<Boolean> nullPages = Collections.nCopies(firstRows.length, false);
        return new ColumnPageStats(
                PrimitiveKind.FLOAT,
                new ColumnIndex(
                        nullPages,
                        segments(mins),
                        segments(maxs),
                        BoundaryOrder.UNORDERED,
                        Optional.of(boxed(nullCounts)),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                offsets(firstRows));
    }

    /** A leaf whose page {@code nullPage} is entirely null: no min/max for that page. */
    private static ColumnPageStats floatLeafWithNullPage(long[] firstRows, int nullPage) {
        List<Boolean> nullPages = new ArrayList<>();
        List<MemorySegment> mins = new ArrayList<>();
        List<MemorySegment> maxs = new ArrayList<>();
        for (int i = 0; i < firstRows.length; i++) {
            boolean isNull = i == nullPage;
            nullPages.add(isNull);
            mins.add(isNull ? MemorySegment.NULL : bytes(0f));
            maxs.add(isNull ? MemorySegment.NULL : bytes(1f));
        }
        return new ColumnPageStats(
                PrimitiveKind.FLOAT,
                new ColumnIndex(
                        nullPages,
                        mins,
                        maxs,
                        BoundaryOrder.UNORDERED,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                offsets(firstRows));
    }

    private static OffsetIndex offsets(long[] firstRows) {
        List<PageLocation> locations = new ArrayList<>();
        for (int i = 0; i < firstRows.length; i++) {
            locations.add(new PageLocation(100L * i, 100, firstRows[i]));
        }
        return new OffsetIndex(locations, Optional.empty());
    }

    private static List<MemorySegment> segments(float[] values) {
        List<MemorySegment> out = new ArrayList<>();
        for (float value : values) {
            out.add(bytes(value));
        }
        return out;
    }

    private static MemorySegment bytes(float value) {
        byte[] raw = ByteBuffer.allocate(4)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putFloat(value)
                .array();
        return MemorySegment.ofArray(raw).asReadOnly();
    }

    private static List<Long> boxed(long[] values) {
        return Arrays.stream(values).boxed().toList();
    }

    private static long[] rows(long... v) {
        return v;
    }

    private static float[] mins(float... v) {
        return v;
    }

    private static float[] maxs(float... v) {
        return v;
    }

    private static long[] nulls(long... v) {
        return v;
    }
}
