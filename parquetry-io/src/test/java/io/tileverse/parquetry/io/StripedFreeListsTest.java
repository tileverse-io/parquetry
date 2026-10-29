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
package io.tileverse.parquetry.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class StripedFreeListsTest {

    @Test
    void defaultCountFollowsTheCores() {
        assertThat(StripedFreeLists.defaultStripeCount(1)).isEqualTo(1);
        assertThat(StripedFreeLists.defaultStripeCount(4)).isEqualTo(4);
        assertThat(StripedFreeLists.defaultStripeCount(6)).isEqualTo(8);
        assertThat(StripedFreeLists.defaultStripeCount(32)).isEqualTo(32);
        assertThat(StripedFreeLists.defaultStripeCount(48)).isEqualTo(64);
        assertThat(StripedFreeLists.defaultStripeCount(256)).isEqualTo(64);
    }

    // A stripe's share of the small cap must hold the largest small buffers; a small cap gets fewer stripes.
    @Test
    void smallCapsGetFewerStripes() {
        long megabyte = 1L << 20;
        assertThat(StripedFreeLists.stripeCount(16, 512 * megabyte, 4 * megabyte))
                .isEqualTo(16);
        assertThat(StripedFreeLists.stripeCount(16, 16 * megabyte, 4 * megabyte))
                .isEqualTo(2);
        assertThat(StripedFreeLists.stripeCount(16, 4 * megabyte, 4 * megabyte)).isEqualTo(1);
        assertThat(StripedFreeLists.stripeCount(16, megabyte, 8192)).isEqualTo(16);
        assertThat(StripedFreeLists.stripeCount(16, 0, 8192)).isEqualTo(1);
    }

    // A thread masks its id to find its stripe, and a mask needs a power of two.
    @Test
    void rejectsNonPowerOfTwoCounts() {
        assertThatThrownBy(() -> new StripedFreeLists(1 << 20, 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("power of two");
        assertThatThrownBy(() -> new StripedFreeLists(1 << 20, 0)).isInstanceOf(IllegalArgumentException.class);
    }
}
