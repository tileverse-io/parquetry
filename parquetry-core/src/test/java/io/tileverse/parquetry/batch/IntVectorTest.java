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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.BitSet;

import org.junit.jupiter.api.Test;

class IntVectorTest {

    @Test
    void getIntReturnsTheValueForAValidRow() {
        IntVector vec = IntVector.materialized(new int[] {10, 20}, Validity.allValid(2));
        assertThat(vec.getInt(1)).as("valid row returns its value").isEqualTo(20);
    }

    @Test
    void getIntFailsFastOnANullRow() {
        BitSet bits = new BitSet(2);
        bits.set(0);
        IntVector vec = IntVector.materialized(new int[] {10, 0}, Validity.of(bits, 2));
        assertThatThrownBy(() -> vec.getInt(1))
                .as("a null row must not silently return the parked default 0")
                .isInstanceOf(IllegalStateException.class);
    }
}
