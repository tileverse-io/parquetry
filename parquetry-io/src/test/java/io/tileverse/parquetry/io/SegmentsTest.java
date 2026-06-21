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
}
