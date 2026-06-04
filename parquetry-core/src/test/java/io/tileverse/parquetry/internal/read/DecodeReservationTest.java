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

import org.junit.jupiter.api.Test;

class DecodeReservationTest {

    @Test
    void releaseReturnsBytesToBudgetExactlyOnce() {
        DecodeBudget budget = DecodeBudget.ofBytes(1_000);
        budget.tryReserve(400);
        DecodeReservation reservation = new DecodeReservation(budget, 400);

        reservation.release();
        assertThat(budget.available()).isEqualTo(1_000);

        reservation.release();
        assertThat(budget.available()).isEqualTo(1_000);
    }

    @Test
    void noneReleaseIsNoOp() {
        DecodeReservation.NONE.release();
        assertThat(DecodeReservation.NONE.bytes()).isZero();
    }
}
