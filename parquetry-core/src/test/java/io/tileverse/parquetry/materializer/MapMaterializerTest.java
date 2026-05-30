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
package io.tileverse.parquetry.materializer;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.MapVector;

class MapMaterializerTest {

    @Test
    void buildsLinkedHashMapPreservingInsertionOrder() {
        BinaryVector keys = BinaryVector.materialized(
                new MemorySegment[] {MemorySegment.ofArray("a".getBytes()), MemorySegment.ofArray("b".getBytes())},
                allValid(2));
        IntVector values = IntVector.materialized(new int[] {1, 2}, allValid(2));
        int[] offsets = {0, 2, 2};
        BitSet validity = new BitSet(2);
        validity.set(0, 2);
        MapVector vec = new MapVector(offsets, keys, values, validity, 2);

        Map<?, ?> result = MapMaterializer.materializeAt(vec, 0, null);
        assertThat(result).hasSize(2);
        Iterator<?> keyIt = result.keySet().iterator();
        MemorySegment k1 = (MemorySegment) keyIt.next();
        MemorySegment k2 = (MemorySegment) keyIt.next();
        assertThat(k1.toArray(JAVA_BYTE)).isEqualTo("a".getBytes());
        assertThat(k2.toArray(JAVA_BYTE)).isEqualTo("b".getBytes());
    }

    @Test
    void nullRowReturnsNull() {
        BinaryVector keys =
                BinaryVector.materialized(new MemorySegment[] {MemorySegment.ofArray("a".getBytes())}, allValid(1));
        IntVector values = IntVector.materialized(new int[] {1}, allValid(1));
        int[] offsets = {0, 1, 1};
        BitSet validity = new BitSet(2);
        validity.set(0); // row 1 null

        MapVector vec = new MapVector(offsets, keys, values, validity, 2);

        assertThat(MapMaterializer.materializeAt(vec, 0, null)).hasSize(1);
        assertThat(MapMaterializer.materializeAt(vec, 1, null)).isNull();
    }

    @Test
    void emptyMapDistinguishedFromNull() {
        BinaryVector keys = BinaryVector.materialized(new MemorySegment[0], new BitSet(0));
        IntVector values = IntVector.materialized(new int[0], new BitSet(0));
        int[] offsets = {0, 0};
        BitSet validity = new BitSet(1);
        validity.set(0); // non-null but empty

        MapVector vec = new MapVector(offsets, keys, values, validity, 1);

        Map<?, ?> result = MapMaterializer.materializeAt(vec, 0, null);
        assertThat(result).isNotNull().isEmpty();
    }

    @Test
    void nullPrimitiveValueMaterializesAsNull() {
        BinaryVector keys =
                BinaryVector.materialized(new MemorySegment[] {MemorySegment.ofArray("a".getBytes())}, allValid(1));
        // The single value is null: validity bit clear, backing slot parks the default 0.
        IntVector values = IntVector.materialized(new int[] {0}, new BitSet(1));
        int[] offsets = {0, 1};
        BitSet validity = new BitSet(1);
        validity.set(0);
        MapVector vec = new MapVector(offsets, keys, values, validity, 1);

        Map<?, ?> result = MapMaterializer.materializeAt(vec, 0, null);

        assertThat(result).hasSize(1);
        assertThat(result.entrySet().iterator().next().getValue())
                .as("a null primitive map value reads back as null, not the default 0")
                .isNull();
    }

    private static BitSet allValid(int n) {
        BitSet b = new BitSet(n);
        b.set(0, n);
        return b;
    }
}
