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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.schema.ColumnPath;

class CoalescingFetchPlannerTest {

    private static final int GAP = 1_000;
    private static final int SPAN = 10_000;

    private static ColumnPath path(String name) {
        return ColumnPath.of(name);
    }

    private static FetchUnit unit(String name, long offset, int length) {
        return new FetchUnit(path(name), offset, length, 0, false);
    }

    private static List<RunSlice> runsOf(FetchPlan plan, String name) {
        return plan.slices().get(path(name)).runs();
    }

    @Test
    void mergesAdjacentChunksWithinGapAndSpan() {
        FetchPlan plan = CoalescingFetchPlanner.plan(
                List.of(unit("a", 0, 100), unit("b", 100, 100), unit("c", 300, 100)), GAP, SPAN);

        assertThat(plan.ranges()).containsExactly(new CoalescedRange(0, 400));
        assertThat(runsOf(plan, "a")).containsExactly(new RunSlice(0, 0, 100, 0));
        assertThat(runsOf(plan, "b")).containsExactly(new RunSlice(0, 100, 100, 0));
        assertThat(runsOf(plan, "c")).containsExactly(new RunSlice(0, 300, 100, 0));
        assertThat(plan.totalBytes()).isEqualTo(400);
    }

    @Test
    void startsNewRangeWhenGapTooLarge() {
        FetchPlan plan =
                CoalescingFetchPlanner.plan(List.of(unit("a", 0, 100), unit("b", 100 + GAP + 1, 100)), GAP, SPAN);

        assertThat(plan.ranges()).containsExactly(new CoalescedRange(0, 100), new CoalescedRange(100 + GAP + 1, 100));
        assertThat(runsOf(plan, "b")).containsExactly(new RunSlice(1, 0, 100, 0));
    }

    @Test
    void startsNewRangeWhenSpanWouldBeExceeded() {
        FetchPlan plan =
                CoalescingFetchPlanner.plan(List.of(unit("a", 0, SPAN - 10), unit("b", SPAN - 10, 100)), GAP, SPAN);

        assertThat(plan.ranges()).containsExactly(new CoalescedRange(0, SPAN - 10), new CoalescedRange(SPAN - 10, 100));
    }

    @Test
    void singleChunkLargerThanSpanBecomesItsOwnRange() {
        FetchPlan plan = CoalescingFetchPlanner.plan(List.of(unit("a", 0, SPAN + 5_000)), GAP, SPAN);

        assertThat(plan.ranges()).containsExactly(new CoalescedRange(0, SPAN + 5_000));
        assertThat(runsOf(plan, "a")).containsExactly(new RunSlice(0, 0, SPAN + 5_000, 0));
    }

    @Test
    void emptyInputProducesEmptyPlan() {
        FetchPlan plan = CoalescingFetchPlanner.plan(List.of(), GAP, SPAN);
        assertThat(plan.ranges()).isEmpty();
        assertThat(plan.slices()).isEmpty();
        assertThat(plan.totalBytes()).isZero();
    }

    @Test
    void sortsUnorderedInputByOffsetBeforeCoalescing() {
        FetchPlan plan = CoalescingFetchPlanner.plan(
                List.of(unit("c", 200, 100), unit("a", 0, 100), unit("b", 100, 100)), GAP, SPAN);

        assertThat(plan.ranges()).containsExactly(new CoalescedRange(0, 300));
        assertThat(runsOf(plan, "a")).containsExactly(new RunSlice(0, 0, 100, 0));
        assertThat(runsOf(plan, "c")).containsExactly(new RunSlice(0, 200, 100, 0));
    }

    @Test
    void multipleUnitsOfOneColumnAppendRunSlicesInOffsetOrder() {
        FetchPlan plan = CoalescingFetchPlanner.plan(
                List.of(new FetchUnit(path("a"), 0, 100, 0, false), new FetchUnit(path("a"), 500, 80, 3, false)),
                GAP,
                SPAN);
        ColumnSlices slices = plan.slices().get(path("a"));
        assertThat(slices.dictionaryPrefix()).isEmpty();
        assertThat(slices.runs()).hasSize(2);
        assertThat(slices.runs().get(0).firstPageOrdinal()).isZero();
        assertThat(slices.runs().get(1).firstPageOrdinal()).isEqualTo(3);
    }

    @Test
    void dictionaryPrefixLandsOutsideTheRunList() {
        FetchPlan plan = CoalescingFetchPlanner.plan(
                List.of(new FetchUnit(path("a"), 0, 40, 0, true), new FetchUnit(path("a"), 40, 100, 0, false)),
                GAP,
                SPAN);
        ColumnSlices slices = plan.slices().get(path("a"));
        assertThat(slices.dictionaryPrefix()).isPresent();
        assertThat(slices.dictionaryPrefix().orElseThrow().length()).isEqualTo(40);
        assertThat(slices.runs()).hasSize(1);
    }

    @Test
    void runIsNeverSplitBySpanCap() {
        FetchPlan plan =
                CoalescingFetchPlanner.plan(List.of(new FetchUnit(path("a"), 0, SPAN + 5_000, 0, false)), GAP, SPAN);
        assertThat(plan.ranges()).containsExactly(new CoalescedRange(0, SPAN + 5_000));
    }

    @Test
    void runsOfDifferentColumnsInterleaveByOffset() {
        FetchPlan plan = CoalescingFetchPlanner.plan(
                List.of(
                        new FetchUnit(path("a"), 0, 100, 0, false),
                        new FetchUnit(path("b"), 100, 100, 0, false),
                        new FetchUnit(path("a"), 200, 100, 2, false)),
                GAP,
                SPAN);
        assertThat(plan.ranges()).hasSize(1);
        assertThat(plan.slices().get(path("a")).runs()).hasSize(2);
        assertThat(plan.slices().get(path("b")).runs()).hasSize(1);
    }
}
