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
package io.tileverse.parquetry.filter;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class BboxTest {
    private static Bbox box(double minX, double minY, double maxX, double maxY) {
        return Bbox.of2d(minX, minY, maxX, maxY);
    }

    @Test
    void containsIsTrueWhenThisEnclosesOther() {
        assertThat(box(0, 0, 10, 10).contains(box(2, 2, 8, 8))).isTrue();
        assertThat(box(0, 0, 10, 10).contains(box(2, 2, 12, 8))).isFalse();
        assertThat(box(0, 0, 10, 10).contains(box(0, 0, 10, 10))).isTrue();
    }

    @Test
    void coveredByIsTheInverseOfContains() {
        Bbox inner = box(2, 2, 8, 8);
        Bbox outer = box(0, 0, 10, 10);
        assertThat(inner.coveredBy(outer)).isTrue();
        assertThat(outer.coveredBy(inner)).isFalse();
    }

    @Test
    void sameBox2dComparesOnlyTheFourEdges() {
        assertThat(box(1, 2, 3, 4).sameBox2d(box(1, 2, 3, 4))).isTrue();
        assertThat(box(1, 2, 3, 4).sameBox2d(box(1, 2, 3, 5))).isFalse();
        assertThat(Bbox.of3d(1, 2, 9, 3, 4, 9).sameBox2d(box(1, 2, 3, 4))).isTrue();
    }
}
