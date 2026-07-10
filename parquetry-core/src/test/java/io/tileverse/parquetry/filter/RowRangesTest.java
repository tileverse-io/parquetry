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
package io.tileverse.parquetry.filter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.RowRanges.Range;

class RowRangesTest {

    @Test
    void singleRowRangeCountsOne() {
        Range r = new Range(5, 5);
        assertThat(r.count()).isEqualTo(1);
    }

    @Test
    void multiRangeTotalsAreSum() {
        RowRanges rr = new RowRanges(List.of(new Range(0, 9), new Range(20, 29)));
        assertThat(rr.totalRows()).isEqualTo(20);
        assertThat(rr.isEmpty()).isFalse();
    }

    @Test
    void emptyHasZeroRows() {
        RowRanges empty = RowRanges.empty();
        assertThat(empty.totalRows()).isZero();
        assertThat(empty.isEmpty()).isTrue();
    }

    @Test
    void allCreatesContiguousRange() {
        RowRanges rr = RowRanges.all(100);
        assertThat(rr.ranges()).containsExactly(new Range(0, 99));
        assertThat(rr.totalRows()).isEqualTo(100);
    }

    @Test
    void allWithZeroIsEmpty() {
        assertThat(RowRanges.all(0).isEmpty()).isTrue();
    }

    @Test
    void rangeRejectsReversedBounds() {
        assertThatThrownBy(() -> new Range(10, 5)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void intersectOverlappingRanges() {
        RowRanges a = new RowRanges(List.of(new Range(0, 20)));
        RowRanges b = new RowRanges(List.of(new Range(10, 30)));
        assertThat(a.intersect(b).ranges()).containsExactly(new Range(10, 20));
    }

    @Test
    void intersectDisjointRangesYieldsEmpty() {
        RowRanges a = new RowRanges(List.of(new Range(0, 10)));
        RowRanges b = new RowRanges(List.of(new Range(20, 30)));
        assertThat(a.intersect(b).isEmpty()).isTrue();
    }

    @Test
    void intersectMultiRange() {
        RowRanges a = new RowRanges(List.of(new Range(0, 9), new Range(20, 29), new Range(40, 49)));
        RowRanges b = new RowRanges(List.of(new Range(5, 25), new Range(45, 60)));
        assertThat(a.intersect(b).ranges()).containsExactly(new Range(5, 9), new Range(20, 25), new Range(45, 49));
    }
}
