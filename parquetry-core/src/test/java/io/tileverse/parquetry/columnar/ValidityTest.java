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
package io.tileverse.parquetry.columnar;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
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
    void selectAllReturnsTheSameMask() {
        BitSet bits = new BitSet(4);
        bits.set(0, 4);
        bits.clear(2);
        Validity v = Validity.of(bits, 4);
        assertThat(v.select(Selection.ALL)).isSameAs(v);
    }

    @Test
    void selectProjectsNullnessThroughTheIndexMap() {
        // physical rows: 0=valid 1=null 2=valid 3=null 4=valid
        BitSet bits = new BitSet(5);
        bits.set(0);
        bits.set(2);
        bits.set(4);
        Validity base = Validity.of(bits, 5);

        // select physical rows 1,3,4 -> logical 0,1,2
        Validity selected = base.select(Selection.bits(survivors(1, 3, 4)));

        assertThat(selected.size()).as("logical size").isEqualTo(3);
        assertThat(selected.isNull(0)).as("physical 1 was null").isTrue();
        assertThat(selected.isNull(1)).as("physical 3 was null").isTrue();
        assertThat(selected.isValid(2)).as("physical 4 was valid").isTrue();
        assertThat(selected.nullCount()).isEqualTo(2);
    }

    @Test
    void selectOnAllValidStaysAllValidAndAllocatesNoBitmap() {
        Validity base = Validity.allValid(10);
        Validity selected = base.select(Selection.range(3, 4));
        assertThat(selected.size()).isEqualTo(4);
        assertThat(selected.hasNulls()).isFalse();
        assertThat(selected.heapBytes()).as("all-valid holds no bitmap").isZero();
    }

    @Test
    void selectAgreesWithReadingBaseValidityAtTranslatedIndices() {
        BitSet bits = new BitSet(8);
        bits.set(1);
        bits.set(2);
        bits.set(5);
        bits.set(7);
        Validity base = Validity.of(bits, 8);
        Selection selection = Selection.bits(survivors(0, 2, 5, 6, 7));
        Validity selected = base.select(selection);
        for (int logical = 0; logical < selection.length(); logical++) {
            assertThat(selected.isValid(logical))
                    .as("logical %d -> physical %d", logical, selection.physical(logical))
                    .isEqualTo(base.isValid(selection.physical(logical)));
        }
    }

    private static BitSet survivors(int... physicalRows) {
        BitSet bits = new BitSet();
        for (int row : physicalRows) {
            bits.set(row);
        }
        return bits;
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

    @Test
    void segmentBackedReadsValidityBitsAndNullCount() {
        // rows 0..7; rows 2 and 5 null -> bits 0,1,3,4,6,7 set, bits 2,5 clear
        byte bitmap = (byte) 0b1101_1011; // LSB-first: bit0=1,bit1=1,bit2=0,bit3=1,bit4=1,bit5=0,bit6=1,bit7=1
        MemorySegment seg = MemorySegment.ofArray(new byte[] {bitmap});
        Validity validity = Validity.ofSegment(seg, 2, 8);
        assertThat(validity.size()).as("size").isEqualTo(8);
        assertThat(validity.nullCount()).as("nullCount").isEqualTo(2);
        assertThat(validity.hasNulls()).as("hasNulls").isTrue();
        assertThat(validity.isNull(2)).as("row 2 null").isTrue();
        assertThat(validity.isNull(5)).as("row 5 null").isTrue();
        assertThat(validity.isValid(0)).as("row 0 valid").isTrue();
        assertThat(validity.isValid(7)).as("row 7 valid").isTrue();
    }

    @Test
    void segmentBackedCopyMaterializesEquivalentBitSet() {
        byte bitmap = (byte) 0b1101_1011;
        MemorySegment seg = MemorySegment.ofArray(new byte[] {bitmap});
        Validity validity = Validity.ofSegment(seg, 2, 8);
        BitSet copy = validity.copy();
        assertThat(copy.get(0)).isTrue();
        assertThat(copy.get(2)).isFalse();
        assertThat(copy.get(5)).isFalse();
        assertThat(copy.cardinality()).as("valid rows").isEqualTo(6);
    }

    @Test
    void segmentBackedReportsNoHeapBytes() {
        MemorySegment seg = MemorySegment.ofArray(new byte[] {(byte) 0xFF});
        Validity validity = Validity.ofSegment(seg, 0, 8);
        assertThat(validity.heapBytes()).as("off-heap bitmap is not heap").isZero();
    }

    @Test
    void sliceOfFullyValidRangeCollapsesToAllValid() {
        BitSet bits = new BitSet();
        bits.set(0, 10);
        bits.clear(8); // a null outside the sliced range keeps the mask bitmap-backed
        Validity validity = Validity.of(bits, 10);

        Validity slice = validity.slice(3, 4); // rows 3..6, all valid

        assertThat(slice.size()).as("slice row count").isEqualTo(4);
        assertThat(slice.hasNulls()).as("a fully valid range has no nulls").isFalse();
    }

    @Test
    void sliceRebasesNullsToRowZero() {
        BitSet bits = new BitSet();
        bits.set(0, 10);
        bits.clear(5); // row 5 null
        Validity validity = Validity.of(bits, 10);

        Validity slice = validity.slice(3, 4); // rows 3..6 -> local index 2 is null

        assertThat(slice.size()).as("slice row count").isEqualTo(4);
        assertThat(slice.hasNulls()).as("the slice includes a null row").isTrue();
        assertThat(slice.isNull(2)).as("row 5 maps to local index 2").isTrue();
        assertThat(slice.isValid(0)).as("row 3 is valid").isTrue();
        assertThat(slice.nullCount()).as("exactly one null in the slice").isEqualTo(1);
    }

    @Test
    void sliceOfAllValidMaskIsAllValid() {
        Validity slice = Validity.allValid(100).slice(20, 30);
        assertThat(slice.size()).isEqualTo(30);
        assertThat(slice.hasNulls()).isFalse();
    }

    @Test
    void sliceExcludesBitsBeyondTheWindow() {
        BitSet bits = new BitSet();
        bits.set(0, 200);
        bits.clear(40); // null before the window keeps the mask bitmap-backed
        Validity validity = Validity.of(bits, 200);

        Validity slice = validity.slice(50, 64);

        assertThat(slice.size()).isEqualTo(64);
        assertThat(slice.hasNulls()).isFalse();
    }

    @Test
    void sliceOfSegmentBackedMaskMirrorsTheBits() {
        byte bitmap = (byte) 0b1101_1011; // rows 2 and 5 null
        MemorySegment seg = MemorySegment.ofArray(new byte[] {bitmap});
        Validity validity = Validity.ofSegment(seg, 2, 8);

        Validity slice = validity.slice(4, 4); // rows 4..7 -> local index 1 (row 5) null

        assertThat(slice.size()).isEqualTo(4);
        assertThat(slice.nullCount()).isEqualTo(1);
        assertThat(slice.isNull(1)).as("row 5 maps to local index 1").isTrue();
        assertThat(slice.isValid(0)).isTrue();
        assertThat(slice.isValid(2)).isTrue();
    }
}
