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
package io.tileverse.parquetry.variant;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Test;

class VariantMetadataTest {

    @Test
    void readsUnsortedDictionaryWithOneByteOffsets() {
        // header 0x01 (version 1, unsorted, offset_size 1), dict_size 2, offsets [0,1,4], keys "a","bcd"
        byte[] bytes = {0x01, 0x02, 0x00, 0x01, 0x04, 'a', 'b', 'c', 'd'};
        VariantMetadata metadata =
                new VariantMetadata(MemorySegment.ofArray(bytes).asReadOnly());

        assertThat(metadata.dictionarySize()).as("dictionary size").isEqualTo(2);
        assertThat(metadata.key(0)).as("key 0").isEqualTo("a");
        assertThat(metadata.key(1)).as("key 1").isEqualTo("bcd");
        assertThat(metadata.idOf("bcd")).as("lookup present key").isEqualTo(1);
        assertThat(metadata.idOf("zzz")).as("lookup absent key").isEqualTo(-1);
    }

    @Test
    void binarySearchesWhenSortedFlagSet() {
        // header 0x11 (version 1, sorted bit set, offset_size 1), dict_size 2, offsets [0,1,2], keys "a","b"
        byte[] bytes = {0x11, 0x02, 0x00, 0x01, 0x02, 'a', 'b'};
        VariantMetadata metadata =
                new VariantMetadata(MemorySegment.ofArray(bytes).asReadOnly());

        assertThat(metadata.idOf("a")).as("sorted lookup a").isZero();
        assertThat(metadata.idOf("b")).as("sorted lookup b").isEqualTo(1);
        assertThat(metadata.idOf("c")).as("sorted lookup absent").isEqualTo(-1);
    }
}
