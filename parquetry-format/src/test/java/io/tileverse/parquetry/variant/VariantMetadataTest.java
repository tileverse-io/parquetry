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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.ParquetFormatException;

class VariantMetadataTest {

    @Test
    void readsUnsortedDictionaryWithOneByteOffsets() {
        // header 0x01 (version 1, unsorted, offset_size 1), dict_size 2, offsets [0,1,4], keys "a","bcd"
        byte[] bytes = {0x01, 0x02, 0x00, 0x01, 0x04, 'a', 'b', 'c', 'd'};
        VariantMetadata metadata = metadata(bytes);

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
        VariantMetadata metadata = metadata(bytes);

        assertThat(metadata.idOf("a")).as("sorted lookup a").isZero();
        assertThat(metadata.idOf("b")).as("sorted lookup b").isEqualTo(1);
        assertThat(metadata.idOf("c")).as("sorted lookup absent").isEqualTo(-1);
    }

    static Stream<Arguments> malformedBuffers() {
        return Stream.of(
                malformed("empty buffer", new byte[0], "is empty"),
                malformed("version 2 header", bytes(0x02, 0x00, 0x00), "version 2"),
                malformed("dictionary size cut short", bytes(0xC1, 0xFF), "too short"),
                malformed(
                        "dictionary size above the signed int range",
                        bytes(0xC1, 0xFF, 0xFF, 0xFF, 0xFF),
                        "4294967295 keys"),
                malformed("offset table larger than the buffer", bytes(0x01, 0xFF, 0x00), "255 keys"),
                malformed("key bytes ending past the buffer", bytes(0x01, 0x01, 0x00, 0x05, 'a'), "end at offset 5"));
    }

    private static Arguments malformed(String description, byte[] bytes, String messageFragment) {
        return Arguments.of(description, bytes, messageFragment);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("malformedBuffers")
    void rejectsMalformedStructureOnConstruction(String description, byte[] bytes, String messageFragment) {
        assertThatThrownBy(() -> metadata(bytes))
                .as(description)
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining(messageFragment);
    }

    @Test
    void checksTheOffsetsOfAKeyWhenTheKeyIsRead() {
        // dict_size 2, offsets [0, 5, 3]: key 0 spans "ABCDE", key 1 would span backwards from 5 to 3
        byte[] bytes = {0x01, 0x02, 0x00, 0x05, 0x03, 'A', 'B', 'C', 'D', 'E'};
        VariantMetadata metadata = metadata(bytes);

        assertThat(metadata.key(0)).as("well-formed key").isEqualTo("ABCDE");
        assertThatThrownBy(() -> metadata.key(1))
                .as("key with decreasing offsets")
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("key 1 has offsets [5, 3)");
    }

    @Test
    void rejectsKeyOffsetsPastTheKeyBytes() {
        // dict_size 2, offsets [0, 9, 2]: the last offset is in range, the middle one is not
        byte[] bytes = {0x01, 0x02, 0x00, 0x09, 0x02, 'a', 'b'};
        VariantMetadata metadata = metadata(bytes);

        assertThatThrownBy(() -> metadata.key(0))
                .as("key ending past the key bytes")
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("invalid for 2 key bytes");
    }

    private static VariantMetadata metadata(byte[] bytes) {
        return new VariantMetadata(MemorySegment.ofArray(bytes).asReadOnly());
    }

    private static byte[] bytes(int... values) {
        byte[] result = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = (byte) values[i];
        }
        return result;
    }
}
