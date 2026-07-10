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
package io.tileverse.parquetry.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Test;

class ValueCoercionTest {

    @Test
    void coercesNumericWideningAndRejectsOverflow() {
        assertThat(ValueCoercion.asInt((short) 7)).isEqualTo(7);
        assertThat(ValueCoercion.asInt(7L)).isEqualTo(7);
        assertThat(ValueCoercion.asLong(7)).isEqualTo(7L);
        assertThat(ValueCoercion.asDouble(1.5f)).isEqualTo(1.5d);
        assertThatThrownBy(() -> ValueCoercion.asInt(1L + Integer.MAX_VALUE))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("int range");
        assertThatThrownBy(() -> ValueCoercion.asInt(-1L + Integer.MIN_VALUE))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("int range");
    }

    @Test
    void coercesStringsAndBytes() {
        assertThat(ValueCoercion.asString(new StringBuilder("hi"))).isEqualTo("hi");
        assertThat(ValueCoercion.asBytes(new byte[] {1, 2})).containsExactly(1, 2);
        assertThat(ValueCoercion.asBytes(MemorySegment.ofArray(new byte[] {3, 4})))
                .containsExactly(3, 4);
        assertThatThrownBy(() -> ValueCoercion.asString(42))
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("string");
    }
}
