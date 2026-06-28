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

class SortedLongPositionSetTest {

    @Test
    void containsFindsMembersAndRejectsNonMembers() {
        RowPositionSet set = SortedLongPositionSet.of(new long[] {0L, 5L, 6L, 100L});
        assertThat(set.contains(0L)).isTrue();
        assertThat(set.contains(6L)).isTrue();
        assertThat(set.contains(4L)).isFalse();
        assertThat(set.contains(101L)).isFalse();
    }

    @Test
    void intersectsDetectsAnyMemberInHalfOpenRange() {
        RowPositionSet set = SortedLongPositionSet.of(new long[] {5L, 6L});
        assertThat(set.intersects(0L, 5L)).isFalse(); // [0,5) excludes 5
        assertThat(set.intersects(0L, 6L)).isTrue(); // [0,6) includes 5
        assertThat(set.intersects(7L, 100L)).isFalse();
    }

    @Test
    void containsRangeIsTrueOnlyWhenEveryPositionPresent() {
        RowPositionSet set = SortedLongPositionSet.of(new long[] {5L, 6L, 7L});
        assertThat(set.containsRange(5L, 8L)).isTrue(); // 5,6,7 all present
        assertThat(set.containsRange(5L, 9L)).isFalse(); // 8 missing
    }

    @Test
    void emptySetIsEmptyAndMatchesNothing() {
        RowPositionSet set = SortedLongPositionSet.of(new long[0]);
        assertThat(set.isEmpty()).isTrue();
        assertThat(set.cardinality()).isZero();
        assertThat(set.contains(0L)).isFalse();
        assertThat(set.intersects(0L, 1000L)).isFalse();
    }

    @Test
    void ofDefensivelyCopiesSortsAndDeduplicates() {
        RowPositionSet unsortedWithDuplicates = SortedLongPositionSet.of(new long[] {6L, 5L, 5L, 0L});
        RowPositionSet canonical = SortedLongPositionSet.of(new long[] {0L, 5L, 6L});

        assertThat(unsortedWithDuplicates.cardinality()).isEqualTo(3L);
        assertThat(unsortedWithDuplicates.contains(0L)).isEqualTo(canonical.contains(0L));
        assertThat(unsortedWithDuplicates.contains(5L)).isEqualTo(canonical.contains(5L));
        assertThat(unsortedWithDuplicates.contains(6L)).isEqualTo(canonical.contains(6L));
        assertThat(unsortedWithDuplicates.contains(4L)).isEqualTo(canonical.contains(4L));
    }

    @Test
    void ofDoesNotRetainTheCallerArray() {
        long[] input = {3L, 1L, 2L};
        RowPositionSet set = SortedLongPositionSet.of(input);

        input[0] = 999L;

        assertThat(set.contains(999L)).isFalse();
        assertThat(set.contains(3L)).isTrue();
    }

    @Test
    void emptyRangeIsTriviallyContainedAndNeverIntersects() {
        RowPositionSet set = SortedLongPositionSet.of(new long[] {5L, 6L, 7L});

        assertThat(set.containsRange(5L, 5L)).isTrue();
        assertThat(set.containsRange(10L, 3L)).isTrue();
        assertThat(set.intersects(5L, 5L)).isFalse();
        assertThat(set.intersects(10L, 3L)).isFalse();
    }
}
