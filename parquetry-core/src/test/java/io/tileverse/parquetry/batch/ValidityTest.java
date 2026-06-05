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
package io.tileverse.parquetry.batch;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.BitSet;

import org.junit.jupiter.api.Test;

class ValidityTest {

    @Test
    void allValidReportsNoNullsAndAllocatesNoBitmap() {
        Validity v = Validity.allValid(5);
        assertThat(v.size()).as("row count").isEqualTo(5);
        assertThat(v.hasNulls()).as("all-valid has no nulls").isFalse();
        assertThat(v.nullCount()).as("null count").isZero();
        assertThat(v.isValid(0)).as("row 0 valid").isTrue();
        assertThat(v.isNull(4)).as("row 4 not null").isFalse();
        assertThat(v.cardinality()).as("all rows count").isEqualTo(5);
    }

    @Test
    void allValidNextSetBitWalksEveryRowThenStops() {
        Validity v = Validity.allValid(3);
        assertThat(v.nextSetBit(0)).isEqualTo(0);
        assertThat(v.nextSetBit(1)).isEqualTo(1);
        assertThat(v.nextSetBit(2)).isEqualTo(2);
        assertThat(v.nextSetBit(3)).as("no row beyond size").isEqualTo(-1);
    }

    @Test
    void ofMirrorsTheBitSetWhenSomeRowsAreNull() {
        BitSet bits = new BitSet(4);
        bits.set(1);
        bits.set(3);
        Validity v = Validity.of(bits, 4);
        assertThat(v.hasNulls()).as("rows 0 and 2 are null").isTrue();
        assertThat(v.nullCount()).isEqualTo(2);
        assertThat(v.isNull(0)).isTrue();
        assertThat(v.isValid(1)).isTrue();
        assertThat(v.nextSetBit(0)).isEqualTo(1);
        assertThat(v.nextSetBit(2)).isEqualTo(3);
    }

    @Test
    void ofCollapsesToAllValidWhenEveryRowIsSet() {
        BitSet bits = new BitSet(3);
        bits.set(0, 3);
        Validity v = Validity.of(bits, 3);
        assertThat(v.hasNulls()).as("a full mask collapses to all-valid").isFalse();
    }

    @Test
    void copyReturnsAnIndependentMutableBitSet() {
        BitSet bits = new BitSet(3);
        bits.set(1);
        Validity v = Validity.of(bits, 3);
        BitSet copy = v.copy();
        copy.set(0);
        assertThat(v.isNull(0)).as("mutating the copy does not affect the mask").isTrue();
    }

    @Test
    void copyOfAllValidIsAFullSet() {
        BitSet copy = Validity.allValid(4).copy();
        assertThat(copy.cardinality()).isEqualTo(4);
    }

    @Test
    void ofClearsStrayBitsBeyondSize() {
        BitSet bits = new BitSet();
        bits.set(1);
        bits.set(5); // stray bit beyond the logical size
        Validity v = Validity.of(bits, 3);
        assertThat(v.size()).as("logical row count").isEqualTo(3);
        assertThat(v.nullCount())
                .as("nullCount must not count the stray bit and must not go negative")
                .isEqualTo(2);
        assertThat(v.isValid(1)).as("row 1 valid").isTrue();
        assertThat(v.isNull(0)).as("row 0 null").isTrue();
        assertThat(v.isNull(2)).as("row 2 null").isTrue();
    }
}
