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
package io.tileverse.parquetry.io;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import org.junit.jupiter.api.Test;

class SegmentsTest {

    @Test
    void copyEqualsSourceBytes() {
        byte[] sourceBytes = {1, 2, 3, 4, 5};
        MemorySegment source = MemorySegment.ofArray(sourceBytes);

        MemorySegment copy = Segments.toHeapReadOnly(source);

        assertThat(copy.toArray(ValueLayout.JAVA_BYTE)).containsExactly(sourceBytes);
    }

    @Test
    void copyIsReadOnly() {
        MemorySegment source = MemorySegment.ofArray(new byte[] {7, 8, 9});

        MemorySegment copy = Segments.toHeapReadOnly(source);

        assertThat(copy.isReadOnly()).isTrue();
    }

    @Test
    void copyDoesNotAliasSource() {
        byte[] sourceBytes = {10, 20, 30};
        MemorySegment source = MemorySegment.ofArray(sourceBytes);

        MemorySegment copy = Segments.toHeapReadOnly(source);
        sourceBytes[0] = 99;

        assertThat(copy.get(ValueLayout.JAVA_BYTE, 0)).isEqualTo((byte) 10);
    }

    @Test
    void readOnlyOrAbsentMapsNullReferenceToAbsentSentinel() {
        assertThat(Segments.readOnlyOrAbsent(null)).isSameAs(MemorySegment.NULL);
    }

    @Test
    void readOnlyOrAbsentKeepsAbsentSentinelIdentity() {
        // The absent sentinel must come back by identity: asReadOnly() would return a fresh zero-length segment that
        // fails the {@code == MemorySegment.NULL} test the format serializers use to omit an optional binary field.
        assertThat(Segments.readOnlyOrAbsent(MemorySegment.NULL)).isSameAs(MemorySegment.NULL);
    }

    @Test
    void readOnlyOrAbsentMakesWritableSegmentReadOnly() {
        MemorySegment writable = MemorySegment.ofArray(new byte[] {1, 2, 3});

        MemorySegment result = Segments.readOnlyOrAbsent(writable);

        assertThat(result.isReadOnly()).isTrue();
        assertThat(result.toArray(ValueLayout.JAVA_BYTE)).containsExactly(1, 2, 3);
    }

    @Test
    void readOnlyOrAbsentReturnsAlreadyReadOnlySegmentUnchanged() {
        MemorySegment readOnly = MemorySegment.ofArray(new byte[] {4, 5, 6}).asReadOnly();

        assertThat(Segments.readOnlyOrAbsent(readOnly)).isSameAs(readOnly);
    }

    @Test
    void readOnlyOrAbsentKeepsAnEmptyPayloadPresent() {
        // A legitimately empty payload (e.g. an empty-string min for a BYTE_ARRAY column) is a present zero-byte value,
        // distinct from the absent sentinel; it must not be folded into MemorySegment.NULL.
        MemorySegment empty = MemorySegment.ofArray(new byte[0]);

        MemorySegment result = Segments.readOnlyOrAbsent(empty);

        assertThat(result).isNotEqualTo(MemorySegment.NULL);
        assertThat(result.byteSize()).isZero();
        assertThat(result.isReadOnly()).isTrue();
    }
}
