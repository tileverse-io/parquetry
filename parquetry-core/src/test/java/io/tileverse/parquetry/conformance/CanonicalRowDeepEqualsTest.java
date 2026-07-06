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
package io.tileverse.parquetry.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class CanonicalRowDeepEqualsTest {

    @Test
    void scalarsAndByteBuffersAndNaN() {
        assertThat(CanonicalRow.deepEquals(7, 7)).as("equal ints").isTrue();
        assertThat(CanonicalRow.deepEquals(7, 8)).as("unequal ints").isFalse();
        assertThat(CanonicalRow.deepEquals(null, null)).as("both null").isTrue();
        assertThat(CanonicalRow.deepEquals(null, 1)).as("null vs present").isFalse();
        assertThat(CanonicalRow.deepEquals(ByteBuffer.wrap(new byte[] {1, 2}), ByteBuffer.wrap(new byte[] {1, 2})))
                .as("equal byte content")
                .isTrue();
        assertThat(CanonicalRow.deepEquals(ByteBuffer.wrap(new byte[] {1}), ByteBuffer.wrap(new byte[] {2})))
                .as("unequal byte content")
                .isFalse();
        assertThat(CanonicalRow.deepEquals(Float.NaN, Float.NaN))
                .as("float NaN equals NaN")
                .isTrue();
        assertThat(CanonicalRow.deepEquals(Double.NaN, Double.NaN))
                .as("double NaN equals NaN")
                .isTrue();
        assertThat(CanonicalRow.deepEquals(1.0f, 2.0f)).as("unequal floats").isFalse();
    }

    @Test
    void listsAndMapsRecurse() {
        assertThat(CanonicalRow.deepEquals(List.of(1, 2), List.of(1, 2)))
                .as("equal lists")
                .isTrue();
        assertThat(CanonicalRow.deepEquals(List.of(1, 2), List.of(1)))
                .as("different size")
                .isFalse();
        assertThat(CanonicalRow.deepEquals(
                        List.of(ByteBuffer.wrap(new byte[] {9})), List.of(ByteBuffer.wrap(new byte[] {9}))))
                .as("list of byte content")
                .isTrue();

        Map<String, Object> a = new LinkedHashMap<>();
        a.put("x", 1);
        a.put("y", List.of(ByteBuffer.wrap(new byte[] {7})));
        Map<String, Object> b = new LinkedHashMap<>();
        b.put("y", List.of(ByteBuffer.wrap(new byte[] {7})));
        b.put("x", 1); // different insertion order
        assertThat(CanonicalRow.deepEquals(a, b))
                .as("maps order-independent, deep values")
                .isTrue();

        Map<String, Object> c = new LinkedHashMap<>();
        c.put("x", 1);
        c.put("y", List.of(ByteBuffer.wrap(new byte[] {8})));
        assertThat(CanonicalRow.deepEquals(a, c))
                .as("maps differ on nested value")
                .isFalse();
    }
}
