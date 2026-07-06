/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class DiskBudgetTest {

    @Test
    void tryReserveSucceedsWithinCapacityAndFailsBeyond() {
        DiskBudget budget = DiskBudget.ofBytes(100);
        assertThat(budget.tryReserve(60)).isTrue();
        assertThat(budget.available()).isEqualTo(40);
        assertThat(budget.tryReserve(50)).isFalse();
        assertThat(budget.available()).isEqualTo(40);
    }

    @Test
    void releaseReturnsHeadroomClampedToCapacity() {
        DiskBudget budget = DiskBudget.ofBytes(100);
        budget.tryReserve(80);
        budget.release(200);
        assertThat(budget.available()).isEqualTo(100);
    }

    @Test
    void reserveGivesUpWhenAskedRatherThanBlockingForever() {
        DiskBudget budget = DiskBudget.ofBytes(10);
        boolean reserved = budget.reserve(50, () -> true);
        assertThat(reserved).isFalse();
        assertThat(budget.available()).isEqualTo(10);
    }

    @Test
    void nonPositiveRequestReservesNothing() {
        DiskBudget budget = DiskBudget.ofBytes(10);
        assertThat(budget.tryReserve(0)).isTrue();
        assertThat(budget.available()).isEqualTo(10);
    }

    @Test
    void defaultBudgetHasPositiveCapacity() {
        assertThat(DiskBudget.defaultBudget().capacity()).isPositive();
    }
}
