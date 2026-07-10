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
package io.tileverse.parquetry.columnar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.BitSet;

import org.junit.jupiter.api.Test;

class GenericGetTest {

    @Test
    void primitiveGetBoxesValueAndReturnsNullOnNullRow() {
        BitSet bits = new BitSet(2);
        bits.set(0);
        IntVector vec = IntVector.materialized(new int[] {5, 0}, Validity.of(bits, 2));
        Integer present = vec.get(0);
        Integer missing = vec.get(1);
        assertThat(present).as("valid row boxes its value").isEqualTo(5);
        assertThat(missing).as("null row boxes to null").isNull();
    }

    @Test
    void primitiveGetToWrongConcreteTargetThrowsClassCastException() {
        IntVector vec = IntVector.materialized(new int[] {5}, Validity.allValid(1));
        assertThatThrownBy(() -> assignToLong(vec))
                .as("assigning an int vector's boxed value to Long throws at the checkcast")
                .isInstanceOf(ClassCastException.class);
    }

    @SuppressWarnings("unused") // the checkcast on assignment is the behavior under test
    private static Long assignToLong(IntVector vec) {
        return vec.get(0);
    }

    @Test
    void nestedGetThrowsUnsupported() {
        IntVector child = IntVector.materialized(new int[] {1}, Validity.allValid(1));
        ListVector list = new ListVector(new int[] {0, 1}, child, Validity.allValid(1), 1);
        assertThatThrownBy(() -> list.get(0))
                .as("nested vectors materialize through the materializer, not get")
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
