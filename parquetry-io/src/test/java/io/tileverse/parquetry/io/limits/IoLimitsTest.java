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
package io.tileverse.parquetry.io.limits;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;

class IoLimitsTest {

    private static final Path DIR = Path.of("/tmp/spill");

    @Test
    void appliesFractionsToRawFacts() {
        ResourceLimits fake = ResourceLimits.fixed(10L << 30, 100L << 30, 8, DIR);
        IoLimits limits = IoLimits.from(fake);
        assertThat(limits.maxOffHeapBytes()).as("0.1 of 10 GiB").isEqualTo((long) (0.1 * (10L << 30)));
        assertThat(limits.maxSpillBytes()).as("0.5 of 100 GiB").isEqualTo((long) (0.5 * (100L << 30)));
        assertThat(limits.spillDir()).isEqualTo(DIR);
    }

    @Test
    void derivesDecodeBudgetAtTwentyFivePercentOfMemory() {
        ResourceLimits limits = ResourceLimits.fixed(2_000_000_000L, 100_000_000_000L, 8, Path.of("/tmp"));
        IoLimits ioLimits = IoLimits.from(limits);
        assertThat(ioLimits.maxDecodeBytes())
                .as("decode ceiling is 25% of available memory")
                .isEqualTo(500_000_000L);
    }

    @Test
    void capsAreAtLeastOneByte() {
        ResourceLimits tiny = ResourceLimits.fixed(1L, 1L, 1, DIR);
        IoLimits limits = IoLimits.from(tiny);
        assertThat(limits.maxOffHeapBytes()).as("off-heap floor").isEqualTo(1L);
        assertThat(limits.maxSpillBytes()).as("spill floor").isEqualTo(1L);
        assertThat(limits.maxDecodeBytes()).as("decode floor").isEqualTo(1L);
    }

    @Test
    void rejectsNonPositiveCaps() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new IoLimits(0, 1, 1, DIR))
                .withMessageContaining("maxOffHeapBytes");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new IoLimits(1, 0, 1, DIR))
                .withMessageContaining("maxSpillBytes");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new IoLimits(1, 1, 0, DIR))
                .withMessageContaining("maxDecodeBytes");
    }
}
