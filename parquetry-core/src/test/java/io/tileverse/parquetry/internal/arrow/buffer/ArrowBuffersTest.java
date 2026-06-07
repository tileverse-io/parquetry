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
package io.tileverse.parquetry.internal.arrow.buffer;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.BitSet;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.Validity;

class ArrowBuffersTest {

    @Test
    void alignsUpToEightBytes() {
        assertThat(ArrowBuffers.align(0)).isZero();
        assertThat(ArrowBuffers.align(1)).isEqualTo(8);
        assertThat(ArrowBuffers.align(8)).isEqualTo(8);
        assertThat(ArrowBuffers.align(9)).isEqualTo(16);
    }

    @Test
    void allValidValidityEncodesToEmptyBufferAndRestores() {
        Validity allValid = Validity.allValid(5);
        MemorySegment buffer = ArrowBuffers.encodeValidity(allValid);
        assertThat(buffer.byteSize()).isZero();
        Validity restored = ArrowBuffers.decodeValidity(buffer, allValid.nullCount(), 5);
        assertThat(restored.hasNulls()).isFalse();
        assertThat(restored.size()).isEqualTo(5);
    }

    @Test
    void nullsValidityRoundTripsLsbFirst() {
        BitSet valid = new BitSet(5);
        valid.set(0);
        valid.set(2);
        valid.set(4); // rows 1 and 3 null
        Validity withNulls = Validity.of(valid, 5);
        MemorySegment buffer = ArrowBuffers.encodeValidity(withNulls);
        assertThat(buffer.byteSize()).isEqualTo(8);
        Validity restored = ArrowBuffers.decodeValidity(buffer, withNulls.nullCount(), 5);
        assertThat(restored.isNull(1)).isTrue();
        assertThat(restored.isNull(3)).isTrue();
        assertThat(restored.isValid(0)).isTrue();
        assertThat(restored.nullCount()).isEqualTo(2);
    }
}
