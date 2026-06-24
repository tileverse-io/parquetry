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
package io.tileverse.parquetry.arrow.cdi;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Test;

class CallbackRegistryTest {

    @Test
    void registerThenLookupReturnsTheSameState() {
        CallbackRegistry<String> registry = new CallbackRegistry<>();

        MemorySegment key = registry.register("state");

        assertThat(registry.lookup(key)).isEqualTo("state");
        assertThat(registry.outstanding()).isEqualTo(1);
    }

    @Test
    void removeReturnsTheStateOnceThenNull() {
        CallbackRegistry<String> registry = new CallbackRegistry<>();
        MemorySegment key = registry.register("state");

        assertThat(registry.remove(key)).isEqualTo("state");
        assertThat(registry.remove(key)).isNull();
        assertThat(registry.outstanding()).isZero();
    }

    @Test
    void distinctRegistrationsGetDistinctKeysAndCounts() {
        CallbackRegistry<String> registry = new CallbackRegistry<>();

        MemorySegment first = registry.register("a");
        MemorySegment second = registry.register("b");

        assertThat(first.address()).isNotEqualTo(second.address());
        assertThat(registry.outstanding()).isEqualTo(2);
        assertThat(registry.lookup(first)).isEqualTo("a");
        assertThat(registry.lookup(second)).isEqualTo("b");
    }
}
