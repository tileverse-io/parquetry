/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.data.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.schema.ColumnPath;

class CoalescingFetchPlannerTest {

    private static final int GAP = 1_000;
    private static final int SPAN = 10_000;

    private static ColumnRange col(String name, long offset, int length) {
        return new ColumnRange(ColumnPath.of(name), offset, length);
    }

    @Test
    void mergesAdjacentChunksWithinGapAndSpan() {
        FetchPlan plan = CoalescingFetchPlanner.plan(
                List.of(col("a", 0, 100), col("b", 100, 100), col("c", 300, 100)), GAP, SPAN);

        assertThat(plan.ranges()).containsExactly(new CoalescedRange(0, 400));
        assertThat(plan.slices()).containsEntry(ColumnPath.of("a"), new ColumnSlice(0, 0, 100));
        assertThat(plan.slices()).containsEntry(ColumnPath.of("b"), new ColumnSlice(0, 100, 100));
        assertThat(plan.slices()).containsEntry(ColumnPath.of("c"), new ColumnSlice(0, 300, 100));
        assertThat(plan.totalBytes()).isEqualTo(400);
    }

    @Test
    void startsNewRangeWhenGapTooLarge() {
        FetchPlan plan =
                CoalescingFetchPlanner.plan(List.of(col("a", 0, 100), col("b", 100 + GAP + 1, 100)), GAP, SPAN);

        assertThat(plan.ranges()).containsExactly(new CoalescedRange(0, 100), new CoalescedRange(100 + GAP + 1, 100));
        assertThat(plan.slices()).containsEntry(ColumnPath.of("b"), new ColumnSlice(1, 0, 100));
    }

    @Test
    void startsNewRangeWhenSpanWouldBeExceeded() {
        FetchPlan plan =
                CoalescingFetchPlanner.plan(List.of(col("a", 0, SPAN - 10), col("b", SPAN - 10, 100)), GAP, SPAN);

        assertThat(plan.ranges()).containsExactly(new CoalescedRange(0, SPAN - 10), new CoalescedRange(SPAN - 10, 100));
    }

    @Test
    void singleChunkLargerThanSpanBecomesItsOwnRange() {
        FetchPlan plan = CoalescingFetchPlanner.plan(List.of(col("a", 0, SPAN + 5_000)), GAP, SPAN);

        assertThat(plan.ranges()).containsExactly(new CoalescedRange(0, SPAN + 5_000));
        assertThat(plan.slices()).containsEntry(ColumnPath.of("a"), new ColumnSlice(0, 0, SPAN + 5_000));
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
                List.of(col("c", 200, 100), col("a", 0, 100), col("b", 100, 100)), GAP, SPAN);

        assertThat(plan.ranges()).containsExactly(new CoalescedRange(0, 300));
        assertThat(plan.slices()).containsEntry(ColumnPath.of("a"), new ColumnSlice(0, 0, 100));
        assertThat(plan.slices()).containsEntry(ColumnPath.of("c"), new ColumnSlice(0, 200, 100));
    }
}
