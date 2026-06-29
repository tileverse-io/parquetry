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
package io.tileverse.parquetry.internal.filter;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.RowRanges.Range;
import io.tileverse.parquetry.filter.SortedLongPositionSet;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.explain.Tier;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Unit-level checks for the position-range pruning of a {@link Predicate.RowIndexExcluded} leaf at row-group and page
 * granularity. Each case constructs the smallest possible inputs (a base offset, a row count, and optional page spans)
 * and asserts the three-way verdict: eliminate the whole group, pass every row untouched, or narrow to surviving pages.
 */
class RowPositionEvaluatorTest {

    private static final long ROWS = 1_000;
    private static final ColumnPath POS = ColumnPath.of("_pos");

    @Test
    void everyRowDeletedEliminatesTheRowGroup() {
        Predicate predicate = excluding(rangeOfPositions(0, ROWS));
        PruningDecision decision = RowPositionEvaluator.evaluateRowGroup(predicate, 0L, ROWS);
        assertThat(decision).isInstanceOf(PruningDecision.Eliminated.class);
        assertThat(decision.tier()).isEqualTo(Tier.STATS);
    }

    @Test
    void everyRowDeletedRespectsTheBaseOffset() {
        long base = 5_000L;
        Predicate predicate = excluding(rangeOfPositions(base, base + ROWS));
        PruningDecision decision = RowPositionEvaluator.evaluateRowGroup(predicate, base, ROWS);
        assertThat(decision).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void noDeleteTouchingTheGroupPassesAll() {
        Predicate predicate = excluding(5_000L, 6_000L);
        PruningDecision decision = RowPositionEvaluator.evaluateRowGroup(predicate, 0L, ROWS);
        assertThat(decision).isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void emptyDeleteSetPassesAll() {
        Predicate predicate = excluding();
        PruningDecision decision = RowPositionEvaluator.evaluateRowGroup(predicate, 0L, ROWS);
        assertThat(decision).isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void partialDeleteIsInconclusiveAtRowGroupTier() {
        Predicate predicate = excluding(0L, 1L, 2L);
        PruningDecision decision = RowPositionEvaluator.evaluateRowGroup(predicate, 0L, ROWS);
        assertThat(decision).isInstanceOf(PruningDecision.Inconclusive.class);
    }

    @Test
    void nonRowPositionPredicateIsNotApplied() {
        Predicate predicate = Predicate.ALWAYS_TRUE;
        PruningDecision decision = RowPositionEvaluator.evaluateRowGroup(predicate, 0L, ROWS);
        assertThat(decision).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void fullyDeletedPageIsDroppedFromSurvivingRanges() {
        // Three 100-row pages over [0,300); the middle page [100,200) is fully deleted.
        List<Long> pageFirstRows = List.of(0L, 100L, 200L);
        Predicate predicate = excluding(rangeOfPositions(100, 200));
        PruningDecision decision = RowPositionEvaluator.evaluatePages(predicate, 0L, 300L, pageFirstRows);
        assertThat(decision).isInstanceOf(PruningDecision.NarrowedTo.class);
        assertThat(decision.tier()).isEqualTo(Tier.COLUMN_INDEX);
        RowRanges ranges = ((PruningDecision.NarrowedTo) decision).ranges();
        assertThat(ranges.ranges()).containsExactly(new Range(0, 99), new Range(200, 299));
    }

    @Test
    void fullyDeletedPageRespectsTheBaseOffset() {
        long base = 1_000L;
        List<Long> pageFirstRows = List.of(0L, 100L, 200L);
        // Absolute positions [1100,1200) cover the middle page when base is 1000.
        Predicate predicate = excluding(rangeOfPositions(base + 100, base + 200));
        PruningDecision decision = RowPositionEvaluator.evaluatePages(predicate, base, 300L, pageFirstRows);
        assertThat(decision).isInstanceOf(PruningDecision.NarrowedTo.class);
        RowRanges ranges = ((PruningDecision.NarrowedTo) decision).ranges();
        assertThat(ranges.ranges()).containsExactly(new Range(0, 99), new Range(200, 299));
    }

    @Test
    void deletesSpanningOnlyPartOfAPageLeaveItForRecordLevel() {
        // The middle page is only partly deleted; with no page fully deleted nothing narrows here, and the leaf is
        // left to the record-level tier to drop the individual positions.
        List<Long> pageFirstRows = List.of(0L, 100L, 200L);
        Predicate predicate = excluding(rangeOfPositions(100, 150));
        PruningDecision decision = RowPositionEvaluator.evaluatePages(predicate, 0L, 300L, pageFirstRows);
        assertThat(decision).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void everyPageDeletedEliminatesAtPageTier() {
        List<Long> pageFirstRows = List.of(0L, 100L, 200L);
        Predicate predicate = excluding(rangeOfPositions(0, 300));
        PruningDecision decision = RowPositionEvaluator.evaluatePages(predicate, 0L, 300L, pageFirstRows);
        assertThat(decision).isInstanceOf(PruningDecision.Eliminated.class);
    }

    private static Predicate excluding(long... positions) {
        return new Predicate.RowIndexExcluded(POS, SortedLongPositionSet.of(positions));
    }

    private static long[] rangeOfPositions(long loInclusive, long hiExclusive) {
        long[] out = new long[(int) (hiExclusive - loInclusive)];
        for (int i = 0; i < out.length; i++) {
            out[i] = loInclusive + i;
        }
        return out;
    }
}
