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
package io.tileverse.parquetry.observe;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class FetchStatsTest {

    @Test
    void emptyHasAllZeroes() {
        FetchStats empty = FetchStats.EMPTY;

        assertThat(empty.totalBytes()).isZero();
        assertThat(empty.fetchCount()).isZero();
    }

    @Test
    void totalBytesSumsEveryPurpose() {
        FetchStats stats = new FetchStats(10, 20, 30, 40, 50, 3);

        assertThat(stats.totalBytes()).isEqualTo(150);
    }

    @Test
    void combineAddsEveryFieldPairwise() {
        FetchStats a = new FetchStats(1, 2, 3, 4, 5, 1);
        FetchStats b = new FetchStats(10, 20, 30, 40, 50, 2);

        FetchStats sum = a.combine(b);

        assertThat(sum).isEqualTo(new FetchStats(11, 22, 33, 44, 55, 3));
    }
}
