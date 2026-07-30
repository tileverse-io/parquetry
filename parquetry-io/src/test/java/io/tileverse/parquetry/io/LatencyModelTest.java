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
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.junit.jupiter.api.Test;

class LatencyModelTest {

    @Test
    void addsOneRttPerRequestPlusTransferTime() {
        long rtt = 50_000_000L; // 50 ms
        long bandwidth = 10_000_000L; // 10 MB/s
        long wall = LatencyModel.wallNanos(4, 20_000_000L, rtt, bandwidth);
        // 4 * 50ms + 20MB / 10MB/s = 200ms + 2s
        assertThat(wall).isEqualTo(4 * rtt + 2_000_000_000L);
    }

    @Test
    void zeroRequestsIsZeroWall() {
        assertThat(LatencyModel.wallNanos(0, 0, 50_000_000L, 10_000_000L)).isZero();
    }

    @Test
    void fewerBytesWinsAtLowBandwidthEvenWithMoreRequests() {
        long rtt = 20_000_000L;
        long slowBandwidth = 1_000_000L; // 1 MB/s
        long narrowed = LatencyModel.wallNanos(12, 7_400_000L, rtt, slowBandwidth);
        long wholeChunk = LatencyModel.wallNanos(4, 131_600_000L, rtt, slowBandwidth);
        assertThat(narrowed).isLessThan(wholeChunk);
    }

    @Test
    void rejectsNonPositiveBandwidth() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> LatencyModel.wallNanos(4, 1_000L, 50_000_000L, 0))
                .withMessageContaining("0");
    }
}
