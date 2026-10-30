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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.internal.filter.ColumnSummary;
import io.tileverse.parquetry.internal.filter.PageIndex;
import io.tileverse.parquetry.internal.filter.ValueComparison;
import io.tileverse.parquetry.internal.read.RowGroupChunks;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.geo.geoparquet.BboxCovering;
import io.tileverse.parquetry.schema.geo.geoparquet.BboxCoverings;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * The page-level geometry boxes provided for one geometry column by its bbox covering ({@link BboxCoverings}). Each
 * page of the covering's {@code xmin} leaf is a unit whose row span is that page's, and whose box aggregates the
 * per-page minimum and maximum of the four covering leaves over the span: a misaligned leaf contributes each of its
 * pages overlapping the span. A unit has a box only when at least one non-null value lies in the page, which is what a
 * substitute needs; it records nothing about nulls.
 *
 * <p>The indexes come from the row group's memoized sections, which the COLUMN_INDEX tier already loads for a bbox
 * query. A row group whose covering leaves have no usable index yields no units: the read then plans at row group level
 * only.
 */
public final class CoveringPageBounds {

    private final BboxCovering covering;

    private CoveringPageBounds(BboxCovering covering) {
        this.covering = covering;
    }

    /** The page bounds of {@code geometryColumn}, present when the file declares or exposes a bbox covering for it. */
    public static Optional<CoveringPageBounds> resolve(
            ColumnPath geometryColumn, ParquetSchema schema, Optional<GeoParquetMetadata> geo) {
        if (geo.isEmpty()) {
            return Optional.empty();
        }
        Optional<BboxCovering> resolved = BboxCoverings.coveringOf(geometryColumn, schema, geo.orElseThrow());
        return resolved.map(CoveringPageBounds::new);
    }

    /**
     * One unit per page of the covering's {@code xmin} leaf in row order, or an empty list when any of the four leaves
     * has no column index or offset index in this row group, or when an index is inconsistent.
     *
     * <p>The four leaves are named before the first of them is asked for, hence their eight index sections are read
     * together. A leaf already loaded by the COLUMN_INDEX tier is left to the memo.
     */
    public List<PageUnit> pageUnits(RowGroupChunks chunks) {
        List<ColumnPath> leaves = List.of(covering.xmin(), covering.xmax(), covering.ymin(), covering.ymax());
        chunks.warmPageIndexes(leaves);
        Optional<PageIndex> xmin = chunks.pageIndex(covering.xmin());
        Optional<PageIndex> xmax = chunks.pageIndex(covering.xmax());
        Optional<PageIndex> ymin = chunks.pageIndex(covering.ymin());
        Optional<PageIndex> ymax = chunks.pageIndex(covering.ymax());
        if (xmin.isEmpty() || xmax.isEmpty() || ymin.isEmpty() || ymax.isEmpty()) {
            return List.of();
        }
        return unitsOf(xmin.orElseThrow(), xmax.orElseThrow(), ymin.orElseThrow(), ymax.orElseThrow());
    }

    static List<PageUnit> unitsOf(PageIndex xmin, PageIndex xmax, PageIndex ymin, PageIndex ymax) {
        if (!usable(xmin) || !usable(xmax) || !usable(ymin) || !usable(ymax)) {
            return List.of();
        }
        LeafFold minX = new LeafFold(xmin, Bound.MIN);
        LeafFold maxX = new LeafFold(xmax, Bound.MAX);
        LeafFold minY = new LeafFold(ymin, Bound.MIN);
        LeafFold maxY = new LeafFold(ymax, Bound.MAX);
        List<PageUnit> units = new ArrayList<>(xmin.pageCount());
        for (int page = 0; page < xmin.pageCount(); page++) {
            long first = xmin.firstRow(page);
            long last = xmin.lastRow(page);
            Optional<Bbox> box = boxOver(first, last, minX, maxX, minY, maxY);
            units.add(new PageUnit(first, last, box));
        }
        return units;
    }

    /**
     * A page index provides page boxes when it has statistics with bounds in an order applied by this reader, and at
     * least one page.
     */
    private static boolean usable(PageIndex pages) {
        return pages.hasStatistics() && pages.boundsOrdered() && pages.pageCount() > 0;
    }

    /** The box over rows {@code [first, last]}, or empty when a bound is missing, is NaN, or the box is inverted. */
    private static Optional<Bbox> boxOver(
            long first, long last, LeafFold minXLeaf, LeafFold maxXLeaf, LeafFold minYLeaf, LeafFold maxYLeaf) {
        OptionalDouble minX = minXLeaf.over(first, last);
        OptionalDouble maxX = maxXLeaf.over(first, last);
        OptionalDouble minY = minYLeaf.over(first, last);
        OptionalDouble maxY = maxYLeaf.over(first, last);
        if (minX.isEmpty() || maxX.isEmpty() || minY.isEmpty() || maxY.isEmpty()) {
            return Optional.empty();
        }
        Bbox box = Bbox.of2d(minX.getAsDouble(), minY.getAsDouble(), maxX.getAsDouble(), maxY.getAsDouble());
        if (!hasUsableBounds(box)) {
            return Optional.empty();
        }
        return Optional.of(box);
    }

    /**
     * Whether {@code box} has a real extent on both axes: each minimum at or below its maximum. A NaN bound fails both
     * comparisons and is rejected here.
     */
    private static boolean hasUsableBounds(Bbox box) {
        return box.minX() <= box.maxX() && box.minY() <= box.maxY();
    }

    /**
     * Folds one covering leaf's per-page bounds over a run of spans asked in ascending row order. The leaf's pages are
     * themselves in row order, hence the first page overlapping a span never precedes the first page overlapping the
     * span before it: the search for it resumes where the previous span left it, and the whole pass over a row group
     * costs one walk of the leaf rather than one per span. A page overlapping two consecutive spans is left under the
     * cursor and contributes to both.
     */
    private static final class LeafFold {

        private final PageIndex leaf;
        private final Bound bound;
        private int firstOverlapping;

        LeafFold(PageIndex leaf, Bound bound) {
            this.leaf = leaf;
            this.bound = bound;
        }

        /**
         * The minimum of the page minima (or the maximum of the page maxima) over each page overlapping {@code [first,
         * last]}. Empty when an overlapping page has no bound, when no page overlaps the span, or when the leaf is not
         * a floating-point column. A page holding only nulls has no bound.
         */
        OptionalDouble over(long first, long last) {
            skipPagesEndingBefore(first);
            double folded = bound.startingValue();
            boolean anyPageOverlaps = false;
            for (int page = firstOverlapping; page < leaf.pageCount(); page++) {
                if (leaf.firstRow(page) > last) {
                    break;
                }
                ColumnSummary statistics = leaf.statistics(page);
                OptionalDouble value = floating(bound.of(statistics));
                if (value.isEmpty()) {
                    return OptionalDouble.empty();
                }
                folded = bound.fold(folded, value.getAsDouble());
                anyPageOverlaps = true;
            }
            return anyPageOverlaps ? OptionalDouble.of(folded) : OptionalDouble.empty();
        }

        /** Advances the cursor past each page whose last row falls before {@code first}. */
        private void skipPagesEndingBefore(long first) {
            while (firstOverlapping < leaf.pageCount() && leaf.lastRow(firstOverlapping) < first) {
                firstOverlapping++;
            }
        }

        /** The bound as a double, present for the FLOAT and DOUBLE values of a covering leaf. */
        private static OptionalDouble floating(Optional<Value> pageBound) {
            if (pageBound.isEmpty() || !ValueComparison.isFloating(pageBound.orElseThrow())) {
                return OptionalDouble.empty();
            }
            return OptionalDouble.of(ValueComparison.floatingValue(pageBound.orElseThrow()));
        }
    }

    /** Which end of a covering leaf's per-page statistics is read by a fold, and how a fold combines them. */
    private enum Bound {
        MIN {
            @Override
            double startingValue() {
                return Double.POSITIVE_INFINITY;
            }

            @Override
            double fold(double folded, double pageValue) {
                return Math.min(folded, pageValue);
            }

            @Override
            Optional<Value> of(ColumnSummary page) {
                return page.min();
            }
        },
        MAX {
            @Override
            double startingValue() {
                return Double.NEGATIVE_INFINITY;
            }

            @Override
            double fold(double folded, double pageValue) {
                return Math.max(folded, pageValue);
            }

            @Override
            Optional<Value> of(ColumnSummary page) {
                return page.max();
            }
        };

        /** The neutral value that a fold starts from, before any overlapping page has contributed. */
        abstract double startingValue();

        abstract double fold(double folded, double pageValue);

        abstract Optional<Value> of(ColumnSummary page);
    }

    /**
     * A row span of one row group with the box provided by its covering pages.
     *
     * @param firstRow the first row of the span, relative to the row group
     * @param lastRow the last row of the span, inclusive
     * @param box the box enclosing every geometry of the span; empty when a covering page overlapping the span has no
     *     usable bound
     */
    public record PageUnit(long firstRow, long lastRow, Optional<Bbox> box) {}
}
