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
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.runtime.FetchBudget;

class FetchBudgetTest {

    @Test
    void reservesWhenCapacityAvailable() {
        FetchBudget budget = FetchBudget.ofBytes(1_000);
        assertThat(budget.tryReserve(600)).isTrue();
        assertThat(budget.available()).isEqualTo(400);
    }

    @Test
    void refusesReservationThatExceedsRemaining() {
        FetchBudget budget = FetchBudget.ofBytes(1_000);
        assertThat(budget.tryReserve(600)).isTrue();
        assertThat(budget.tryReserve(600)).isFalse();
        assertThat(budget.available()).isEqualTo(400);
    }

    @Test
    void releaseRestoresCapacity() {
        FetchBudget budget = FetchBudget.ofBytes(1_000);
        budget.tryReserve(1_000);
        budget.release(400);
        assertThat(budget.available()).isEqualTo(400);
        assertThat(budget.tryReserve(400)).isTrue();
    }

    @Test
    void reservationLargerThanCapacityNeverSucceeds() {
        FetchBudget budget = FetchBudget.ofBytes(100);
        assertThat(budget.tryReserve(101)).isFalse();
        assertThat(budget.available()).isEqualTo(100);
    }

    @Test
    void rejectsNonPositiveCapacity() {
        assertThatIllegalArgumentException().isThrownBy(() -> FetchBudget.ofBytes(0));
    }

    @Test
    void rejectsFractionOutsideUnitInterval() {
        assertThatIllegalArgumentException().isThrownBy(() -> FetchBudget.ofMaxMemoryFraction(0));
        assertThatIllegalArgumentException().isThrownBy(() -> FetchBudget.ofMaxMemoryFraction(1.5));
    }

    @Test
    void fractionOfMaxMemoryYieldsPositiveCapacity() {
        FetchBudget budget = FetchBudget.ofMaxMemoryFraction(0.1);
        assertThat(budget.capacity()).isPositive();
    }

    @Test
    void reserveZeroBytesAlwaysSucceedsWithoutConsuming() {
        FetchBudget budget = FetchBudget.ofBytes(100);
        assertThat(budget.tryReserve(0)).isTrue();
        assertThat(budget.available()).isEqualTo(100);
    }

    @Test
    void releaseNeverInflatesAboveCapacity() {
        FetchBudget budget = FetchBudget.ofBytes(100);
        budget.tryReserve(40);
        budget.release(40);
        budget.release(40);
        assertThat(budget.available()).isEqualTo(100);
    }

    @Test
    void releaseLargerThanCapacityIsClampedNotOverflowed() {
        FetchBudget budget = FetchBudget.ofBytes(100);
        budget.tryReserve(100);
        budget.release(Long.MAX_VALUE);
        assertThat(budget.available()).isEqualTo(100);
        assertThat(budget.tryReserve(100)).isTrue();
    }

    @Test
    void rejectsNegativeFraction() {
        assertThatIllegalArgumentException().isThrownBy(() -> FetchBudget.ofMaxMemoryFraction(-0.5));
    }
}
