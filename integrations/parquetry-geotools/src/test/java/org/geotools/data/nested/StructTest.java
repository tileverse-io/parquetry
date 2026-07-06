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
package org.geotools.data.nested;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.geotools.data.nested.NestedType.Field;
import org.geotools.data.nested.NestedType.ScalarType;
import org.geotools.data.nested.NestedType.StructType;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link Struct} presents a struct value as a read-only {@link Map} a generic JSON encoder can serialize.
 */
class StructTest {

    private static final StructType LOCALITY_TYPE =
            new StructType(List.of(new Field("locality", new ScalarType(String.class))));

    @Test
    void indexesFieldByName() {
        Struct struct = new Struct(LOCALITY_TYPE, new Object[] {"NYC"});
        assertThat(struct.get("locality")).isEqualTo("NYC");
    }

    @Test
    void presentsAsMap() {
        Struct struct = new Struct(LOCALITY_TYPE, new Object[] {"NYC"});
        assertThat(struct).isInstanceOf(Map.class);
        assertThat(struct.keySet()).isEqualTo(Set.of("locality"));
    }

    @Test
    void roundTripsThroughGenericMap() {
        Struct struct = new Struct(LOCALITY_TYPE, new Object[] {"NYC"});
        Map<String, Object> copy = new HashMap<>(struct);
        assertThat(copy).containsExactly(Map.entry("locality", "NYC"));
    }

    @Test
    void rejectsMutation() {
        Struct struct = new Struct(LOCALITY_TYPE, new Object[] {"NYC"});
        assertThatThrownBy(() -> struct.put("x", "y")).isInstanceOf(UnsupportedOperationException.class);
    }
}
