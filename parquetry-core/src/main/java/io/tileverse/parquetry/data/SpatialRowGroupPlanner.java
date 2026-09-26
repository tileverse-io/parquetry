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
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.RowRanges.Range;
import io.tileverse.parquetry.filter.SpatialReadProbe;
import io.tileverse.parquetry.filter.SpatialReadProbe.Decision;
import io.tileverse.parquetry.internal.filter.spatial.CoveringPageBounds;
import io.tileverse.parquetry.internal.filter.spatial.CoveringPageBounds.PageUnit;
import io.tileverse.parquetry.internal.filter.spatial.SpatialBoundsSource;
import io.tileverse.parquetry.internal.filter.spatial.UnitAcceptance;
import io.tileverse.parquetry.internal.read.RowGroupChunks;
import io.tileverse.parquetry.internal.read.RowGroupNarrowing;
import io.tileverse.parquetry.internal.read.RowGroupPlanner;
import io.tileverse.parquetry.internal.read.RowGroupSurvivor;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * The spatial plan of one row group for a decimated read. Each unit, the row group first and then each covering page
 * once the group descends, is classified from its box: accepted, which means the statistics prove that every non-null
 * geometry of the unit passes the predicate, or not. An accepted unit is offered to the probe's accepted-region
 * consultation, which may substitute it; any other unit is offered to the read-only region consultation. A unit that
 * the probe skips or substitutes contributes nothing; every other unit keeps its whole row span. The kept spans,
 * intersected with the survivor's own ranges, are the narrowing; none left means the group is dropped, and a plan that
 * dropped nothing reads the group exactly as the filter pipeline left it.
 *
 * <p>A unit with no usable box, whether no bounds at all or a box wrapping the antimeridian, is never accepted and
 * never skipped. A row group with no page units, because the file has no covering or the row group has no column index,
 * is planned at row group level only. A row group with no rows at all is left exactly as the filter pipeline left it.
 *
 * <p>The plan decides which rows are fetched and decoded; it proves nothing about an individual row. Every decoded row
 * still goes through the read's own per-row filtering, and through the per-row spatial gate.
 */
final class SpatialRowGroupPlanner implements RowGroupPlanner {

    private final List<RowGroupSurvivor> survivors;
    private final SpatialReadProbe probe;
    private final Predicate predicate;
    private final ColumnPath geometry;
    private final SpatialBoundsSource boundsSource;
    private final Optional<CoveringPageBounds> pageBounds;

    SpatialRowGroupPlanner(
            List<RowGroupSurvivor> survivors,
            SpatialReadProbe probe,
            Predicate normalizedPredicate,
            ColumnPath geometry,
            SpatialBoundsSource boundsSource,
            Optional<CoveringPageBounds> pageBounds) {
        this.survivors = List.copyOf(survivors);
        this.probe = probe;
        this.predicate = normalizedPredicate;
        this.geometry = geometry;
        this.boundsSource = boundsSource;
        this.pageBounds = pageBounds;
    }

    @Override
    public RowGroupNarrowing plan(int consumePosition) {
        return planRowGroup(survivors.get(consumePosition));
    }

    /** Classifies the row group as a whole, descending to its covering pages when it is not dropped. */
    private RowGroupNarrowing planRowGroup(RowGroupSurvivor survivor) {
        if (survivor.numRows() == 0) {
            return RowGroupNarrowing.whole();
        }
        if (decide(rowGroupBox(survivor)) == UnitDecision.DROP) {
            return RowGroupNarrowing.dropped();
        }
        return planPages(survivor);
    }

    private RowGroupNarrowing planPages(RowGroupSurvivor survivor) {
        List<PageUnit> units = pageUnits(survivor.chunks());
        if (units.isEmpty()) {
            return RowGroupNarrowing.whole();
        }
        return narrowing(planUnits(units), survivor);
    }

    /** Classifies each page unit in row order, collecting the whole span of every unit not dropped. */
    private PlannedRows planUnits(List<PageUnit> units) {
        List<Range> kept = new ArrayList<>(units.size());
        boolean reduced = false;
        for (PageUnit unit : units) {
            if (decide(unit.box()) == UnitDecision.DROP) {
                reduced = true;
                continue;
            }
            kept.add(new Range(unit.firstRow(), unit.lastRow()));
        }
        return new PlannedRows(kept, reduced);
    }

    /**
     * The outcome for one unit. A unit whose box proves every non-null geometry inside the query goes to the probe's
     * accepted-region consultation, any other unit to the read-only one. A unit with no usable box proves nothing and
     * goes to neither: it is read as it stands.
     */
    private UnitDecision decide(Optional<Bbox> box) {
        if (box.isEmpty()) {
            return UnitDecision.DESCEND;
        }
        Bbox unitBox = box.orElseThrow();
        if (!hasUsableBounds(unitBox)) {
            return UnitDecision.DESCEND;
        }
        if (UnitAcceptance.accepts(predicate, geometry, unitBox)) {
            return decideAccepted(unitBox);
        }
        return decideUnproven(unitBox);
    }

    /** The outcome for a unit proven inside the query: the probe may skip it, substitute it, or leave it to be read. */
    private UnitDecision decideAccepted(Bbox box) {
        Decision decision = probe.probeAcceptedRegion(box.minX(), box.minY(), box.maxX(), box.maxY());
        return switch (decision) {
            case Decision.Skip _, Decision.Substitute _ -> UnitDecision.DROP;
            case Decision.Keep _, Decision.Descend _ -> UnitDecision.DESCEND;
        };
    }

    /**
     * The outcome for a unit of unknown content: the read-only consultation may drop a unit already covered, and
     * nothing more. A substitute here would stand for rows never proven to satisfy the query, hence it is rejected.
     */
    private UnitDecision decideUnproven(Bbox box) {
        Decision decision = probe.probeRegion(box.minX(), box.minY(), box.maxX(), box.maxY());
        return switch (decision) {
            case Decision.Skip _ -> UnitDecision.DROP;
            case Decision.Keep _, Decision.Descend _ -> UnitDecision.DESCEND;
            case Decision.Substitute _ ->
                throw new UnsupportedOperationException("a unit of unknown content cannot be substituted: "
                        + "Substitute answers the accepted-region consultation only");
        };
    }

    /**
     * The narrowing of the survivor from the rows kept by its plan: intersected with the survivor's own ranges when it
     * has them, and dropped when nothing is left. A plan that dropped no unit leaves the survivor as the filter
     * pipeline left it.
     *
     * <p>The pruning tiers are sound: they eliminate only pages with no matching row. A unit that the plan keeps is
     * read through whatever the tiers left of it, hence the intersection; an empty intersection means the tiers pruned
     * everything that the plan kept.
     */
    private static RowGroupNarrowing narrowing(PlannedRows planned, RowGroupSurvivor survivor) {
        if (!planned.reduced()) {
            return RowGroupNarrowing.whole();
        }
        RowRanges plannedRows = new RowRanges(planned.ranges());
        RowRanges narrowed =
                survivor.survivingRows().map(plannedRows::intersect).orElse(plannedRows);
        if (narrowed.isEmpty()) {
            return RowGroupNarrowing.dropped();
        }
        return RowGroupNarrowing.narrowedTo(narrowed);
    }

    private List<PageUnit> pageUnits(RowGroupChunks chunks) {
        if (pageBounds.isEmpty()) {
            return List.of();
        }
        return pageBounds.orElseThrow().pageUnits(chunks);
    }

    private Optional<Bbox> rowGroupBox(RowGroupSurvivor survivor) {
        return boundsSource.rowGroupBounds(geometry, survivor.index()).map(SpatialReadGates::toBbox);
    }

    /**
     * Whether {@code box} has a real extent on both axes: each minimum at or below its maximum. A NaN bound fails both
     * comparisons, as does a box wrapping the antimeridian, encoded with its minimum east of its maximum.
     */
    private static boolean hasUsableBounds(Bbox box) {
        return box.minX() <= box.maxX() && box.minY() <= box.maxY();
    }

    /**
     * The rows kept by one row group's plan, and whether the plan dropped any unit of it.
     *
     * @param ranges the kept row ranges in row order, disjoint, relative to the row group
     * @param reduced whether any unit was dropped
     */
    private record PlannedRows(List<Range> ranges, boolean reduced) {}

    /** What one unit's box and the probe decided about it. */
    private enum UnitDecision {
        /** Nothing of the unit is read. */
        DROP,
        /** The unit is read as it stands, or classified one level finer. */
        DESCEND
    }
}
