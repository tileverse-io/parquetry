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
package io.tileverse.parquetry.internal.read.page;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PageSizeLimitTest {

    private static final int MIB = 1 << 20;

    @Test
    void floorCoversSmallRowGroups() {
        PageSizeLimit limit = PageSizeLimit.ofRowGroup(1_000);

        assertThat(limit.accepts(64 * MIB)).isTrue();
        assertThat(limit.accepts(64 * MIB + 1)).isFalse();
    }

    @Test
    void rowGroupSizeSetsTheLimit() {
        PageSizeLimit limit = PageSizeLimit.ofRowGroup(200L * MIB);

        assertThat(limit.accepts(200 * MIB)).isTrue();
        assertThat(limit.accepts(200 * MIB + 1)).isFalse();
    }

    @Test
    void noPositiveSizeSetsNoLimit() {
        assertThat(PageSizeLimit.ofRowGroup(0)).isEqualTo(PageSizeLimit.NONE);
        assertThat(PageSizeLimit.ofRowGroup(-1)).isEqualTo(PageSizeLimit.NONE);
        assertThat(PageSizeLimit.NONE.accepts(Integer.MAX_VALUE)).isTrue();
    }
}
