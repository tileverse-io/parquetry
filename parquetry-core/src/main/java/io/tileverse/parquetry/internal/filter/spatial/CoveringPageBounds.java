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

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.internal.filter.FilterPipeline.ColumnPageStats;
import io.tileverse.parquetry.internal.read.RowGroupChunks;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.geo.geoparquet.BboxCovering;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * The page-level geometry boxes provided for one geometry column by its GeoParquet 1.1 {@code bbox} covering. Each page
 * of the covering's {@code xmin} leaf is a unit whose row span is that page's, and whose box aggregates the per-page
 * minimum and maximum of the four covering leaves over the span: a leaf whose pages do not line up contributes every
 * one of its pages overlapping the span. A unit has a box only when at least one non-null value lies in the page, which
 * is what a substitute needs; it records nothing about nulls.
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
        Optional<BboxCovering> resolved = SpatialCoveringRewrite.coveringFor(geometryColumn, schema, geo.orElseThrow());
        return resolved.map(CoveringPageBounds::new);
    }

    /**
     * One unit per page of the covering's {@code xmin} leaf in row order, or an empty list when any of the four leaves
     * has no column index or offset index in this row group, or when an index is inconsistent.
     */
    public List<PageUnit> pageUnits(RowGroupChunks chunks) {
        Optional<ColumnPageStats> xmin = chunks.pageStats(covering.xmin());
        Optional<ColumnPageStats> xmax = chunks.pageStats(covering.xmax());
        Optional<ColumnPageStats> ymin = chunks.pageStats(covering.ymin());
        Optional<ColumnPageStats> ymax = chunks.pageStats(covering.ymax());
        if (xmin.isEmpty() || xmax.isEmpty() || ymin.isEmpty() || ymax.isEmpty()) {
            return List.of();
        }
        return unitsOf(
                xmin.orElseThrow(), xmax.orElseThrow(), ymin.orElseThrow(), ymax.orElseThrow(), chunks.numRows());
    }

    static List<PageUnit> unitsOf(
            ColumnPageStats xmin, ColumnPageStats xmax, ColumnPageStats ymin, ColumnPageStats ymax, long numRows) {
        if (!consistent(xmin) || !consistent(xmax) || !consistent(ymin) || !consistent(ymax)) {
            return List.of();
        }
        LeafFold minX = new LeafFold(xmin, numRows, Bound.MIN);
        LeafFold maxX = new LeafFold(xmax, numRows, Bound.MAX);
        LeafFold minY = new LeafFold(ymin, numRows, Bound.MIN);
        LeafFold maxY = new LeafFold(ymax, numRows, Bound.MAX);
        List<PageLocation> spans = xmin.offsetIndex().pageLocations();
        List<PageUnit> units = new ArrayList<>(spans.size());
        for (int page = 0; page < spans.size(); page++) {
            long first = spans.get(page).firstRowIndex();
            long last = lastRowOf(spans, page, numRows);
            Optional<Bbox> box = boxOver(first, last, minX, maxX, minY, maxY);
            units.add(new PageUnit(first, last, box));
        }
        return units;
    }

    /** A page index is usable when it has at least one page and its column and offset indexes agree on the count. */
    private static boolean consistent(ColumnPageStats stats) {
        ColumnIndex index = stats.columnIndex();
        int pages = index.minValues().size();
        if (pages == 0) {
            return false;
        }
        return index.maxValues().size() == pages
                && index.nullPages().size() == pages
                && stats.offsetIndex().pageLocations().size() == pages;
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

        private final ColumnPageStats leaf;
        private final List<PageLocation> pages;
        private final long numRows;
        private final Bound bound;
        private int firstOverlapping;

        LeafFold(ColumnPageStats leaf, long numRows, Bound bound) {
            this.leaf = leaf;
            this.pages = leaf.offsetIndex().pageLocations();
            this.numRows = numRows;
            this.bound = bound;
        }

        /**
         * The minimum of the page minima (or the maximum of the page maxima) over every page overlapping {@code [first,
         * last]}. Empty when an overlapping page is all-null, when its bound does not decode, or when no page overlaps
         * the span.
         */
        OptionalDouble over(long first, long last) {
            skipPagesEndingBefore(first);
            ColumnIndex index = leaf.columnIndex();
            double folded = bound.startingValue();
            boolean anyPageOverlaps = false;
            for (int page = firstOverlapping; page < pages.size(); page++) {
                if (pages.get(page).firstRowIndex() > last) {
                    break;
                }
                if (allNull(index, page)) {
                    return OptionalDouble.empty();
                }
                OptionalDouble value = decode(bound.pageBound(index, page));
                if (value.isEmpty()) {
                    return OptionalDouble.empty();
                }
                folded = bound.fold(folded, value.getAsDouble());
                anyPageOverlaps = true;
            }
            return anyPageOverlaps ? OptionalDouble.of(folded) : OptionalDouble.empty();
        }

        /** Advances the cursor past every page whose last row falls before {@code first}. */
        private void skipPagesEndingBefore(long first) {
            while (firstOverlapping < pages.size() && lastRowOf(pages, firstOverlapping, numRows) < first) {
                firstOverlapping++;
            }
        }

        /** Whether page {@code page} holds only nulls, which leaves it with no bound to fold. */
        private static boolean allNull(ColumnIndex index, int page) {
            return Boolean.TRUE.equals(index.nullPages().get(page));
        }

        /** The page bound as a double, read in the leaf's own primitive kind. */
        private OptionalDouble decode(MemorySegment pageBound) {
            return CoveringColumnSource.decodeDouble(leaf.kind(), Optional.of(pageBound));
        }
    }

    private static long lastRowOf(List<PageLocation> pages, int page, long numRows) {
        boolean lastPage = page + 1 >= pages.size();
        if (lastPage) {
            return numRows - 1;
        }
        return pages.get(page + 1).firstRowIndex() - 1;
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
            MemorySegment pageBound(ColumnIndex index, int page) {
                return index.minValues().get(page);
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
            MemorySegment pageBound(ColumnIndex index, int page) {
                return index.maxValues().get(page);
            }
        };

        /** The neutral value that a fold starts from, before any overlapping page has contributed. */
        abstract double startingValue();

        abstract double fold(double folded, double pageValue);

        abstract MemorySegment pageBound(ColumnIndex index, int page);
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
