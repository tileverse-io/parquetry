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
package io.tileverse.parquetry.observe;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.time.Duration;

import org.junit.jupiter.api.Test;

class WriteStatsTest {

    @Test
    void compressionRatioIsUncompressedOverCompressed() {
        WriteStats stats = new WriteStats(100, 2, 250, 1000, Duration.ofSeconds(3));

        assertThat(stats.compressionRatio()).isEqualTo(4.0);
    }

    @Test
    void compressionRatioIsZeroWhenNothingCompressed() {
        WriteStats stats = new WriteStats(0, 0, 0, 0, Duration.ZERO);

        assertThat(stats.compressionRatio()).isZero();
    }

    @Test
    void combineSumsRowsGroupsBytesAndDurationsAndRecomputesRatio() {
        WriteStats a = new WriteStats(100, 2, 200, 800, Duration.ofSeconds(2));
        WriteStats b = new WriteStats(50, 1, 100, 200, Duration.ofSeconds(1));

        WriteStats sum = a.combine(b);

        assertThat(sum.totalRows()).isEqualTo(150);
        assertThat(sum.rowGroups()).isEqualTo(3);
        assertThat(sum.compressedBytes()).isEqualTo(300);
        assertThat(sum.uncompressedBytes()).isEqualTo(1000);
        assertThat(sum.wallClock()).isEqualTo(Duration.ofSeconds(3));
        assertThat(sum.compressionRatio()).isEqualTo(1000.0 / 300.0, within(1e-9));
    }
}
