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
package io.tileverse.parquetry.internal.filter;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.UuidConverter;

class UuidValueComparisonTest {

    // A has high bits 0x0000...; B has high bits 0xFFFF... so B is UNSIGNED-GREATER than A (signed would say
    // otherwise).
    private static final UUID A = new UUID(0x0000000000000000L, 1L);
    private static final UUID B = new UUID(0xFFFFFFFFFFFFFFFFL, 0L);

    @Test
    void compareBoxedSegmentActualVsUuidValBound() {
        MemorySegment segA = UuidConverter.toReadOnlySegment(A);
        assertThat(ValueComparison.compareBoxed(segA, new Value.UuidVal(A))).isZero();
        assertThat(ValueComparison.compareBoxed(segA, new Value.UuidVal(B))).isNegative();
        assertThat(ValueComparison.compareBoxed(UuidConverter.toReadOnlySegment(B), new Value.UuidVal(A)))
                .isPositive();
    }

    @Test
    void compareValuesUuidValQueryVsBinaryValBound() {
        // stats min/max for an FLBA column decode to BinaryVal(segment)
        Value boundA = new Value.BinaryVal(UuidConverter.toReadOnlySegment(A));
        assertThat(ValueComparison.compareValues(new Value.UuidVal(A), boundA)).isZero();
        assertThat(ValueComparison.compareValues(new Value.UuidVal(B), boundA)).isPositive();
        assertThat(ValueComparison.compareValues(
                        new Value.UuidVal(A), new Value.BinaryVal(UuidConverter.toReadOnlySegment(B))))
                .isNegative();
    }

    @Test
    void compareBinaryActualVsUuidValBound() {
        MemorySegment segB = UuidConverter.toReadOnlySegment(B);
        assertThat(ValueComparison.compareBinary(segB, new Value.UuidVal(A))).isPositive();
        assertThat(ValueComparison.compareBinary(UuidConverter.toReadOnlySegment(A), new Value.UuidVal(A)))
                .isZero();
    }

    @Test
    void compareValuesTolerantOfShortBinaryBound() {
        // A truncated FLBA row-group statistic from a foreign writer can decode to fewer than 16 bytes. The UUID arm
        // must fall back to tolerant unsigned byte comparison instead of reading past the segment's end.
        MemorySegment fullUuid = UuidConverter.toReadOnlySegment(A);
        byte[] shortBytes = new byte[8];
        MemorySegment.copy(fullUuid, 0L, MemorySegment.ofArray(shortBytes), 0L, shortBytes.length);
        Value shortBound = new Value.BinaryVal(MemorySegment.ofArray(shortBytes).asReadOnly());

        // A shares the leading 8 bytes with shortBound but is longer, hence compares greater (longer wins on a prefix
        // tie under unsigned lexicographic order).
        assertThat(ValueComparison.compareValues(new Value.UuidVal(A), shortBound))
                .isPositive();
    }

    @Test
    void compareBinaryTolerantOfShortActualSegment() {
        byte[] shortBytes = new byte[8];
        MemorySegment.copy(
                UuidConverter.toReadOnlySegment(B), 0L, MemorySegment.ofArray(shortBytes), 0L, shortBytes.length);
        MemorySegment shortActual = MemorySegment.ofArray(shortBytes).asReadOnly();

        // B's leading 8 bytes are 0xFF...; A's are 0x00..., hence the short actual still compares greater on the first
        // differing byte without reading past its 8-byte end.
        assertThat(ValueComparison.compareBinary(shortActual, new Value.UuidVal(A)))
                .isPositive();
    }
}
