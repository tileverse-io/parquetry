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
package io.tileverse.parquetry.arrow.cdi;

import static java.lang.foreign.MemoryLayout.PathElement.groupElement;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Test;

class CDataLayoutsTest {

    @Test
    void arrowSchemaMatchesTheCAbi() {
        assertThat(CDataLayouts.ARROW_SCHEMA.byteSize()).isEqualTo(72);
        assertThat(CDataLayouts.ARROW_SCHEMA.byteOffset(groupElement("format"))).isEqualTo(0);
        assertThat(CDataLayouts.ARROW_SCHEMA.byteOffset(groupElement("flags"))).isEqualTo(24);
        assertThat(CDataLayouts.ARROW_SCHEMA.byteOffset(groupElement("n_children")))
                .isEqualTo(32);
        assertThat(CDataLayouts.ARROW_SCHEMA.byteOffset(groupElement("children")))
                .isEqualTo(40);
        assertThat(CDataLayouts.ARROW_SCHEMA.byteOffset(groupElement("release")))
                .isEqualTo(56);
        assertThat(CDataLayouts.ARROW_SCHEMA.byteOffset(groupElement("private_data")))
                .isEqualTo(64);
    }

    @Test
    void arrowArrayMatchesTheCAbi() {
        assertThat(CDataLayouts.ARROW_ARRAY.byteSize()).isEqualTo(80);
        assertThat(CDataLayouts.ARROW_ARRAY.byteOffset(groupElement("length"))).isEqualTo(0);
        assertThat(CDataLayouts.ARROW_ARRAY.byteOffset(groupElement("n_buffers")))
                .isEqualTo(24);
        assertThat(CDataLayouts.ARROW_ARRAY.byteOffset(groupElement("buffers"))).isEqualTo(40);
        assertThat(CDataLayouts.ARROW_ARRAY.byteOffset(groupElement("release"))).isEqualTo(64);
        assertThat(CDataLayouts.ARROW_ARRAY.byteOffset(groupElement("private_data")))
                .isEqualTo(72);
    }

    @Test
    void arrowArrayStreamMatchesTheCAbi() {
        assertThat(CDataLayouts.ARROW_ARRAY_STREAM.byteSize()).isEqualTo(40);
        assertThat(CDataLayouts.ARROW_ARRAY_STREAM.byteOffset(groupElement("get_schema")))
                .isEqualTo(0);
        assertThat(CDataLayouts.ARROW_ARRAY_STREAM.byteOffset(groupElement("get_next")))
                .isEqualTo(8);
        assertThat(CDataLayouts.ARROW_ARRAY_STREAM.byteOffset(groupElement("release")))
                .isEqualTo(24);
        assertThat(CDataLayouts.ARROW_ARRAY_STREAM.byteOffset(groupElement("private_data")))
                .isEqualTo(32);
    }

    @Test
    void roundTripsLengthAndFormatThroughTheAccessors() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment array = arena.allocate(CDataLayouts.ARROW_ARRAY);
            CDataLayouts.arraySetLength(array, 1234L);
            assertThat(CDataLayouts.arrayLength(array)).isEqualTo(1234L);

            MemorySegment schema = arena.allocate(CDataLayouts.ARROW_SCHEMA);
            MemorySegment formatString = arena.allocateFrom("+s");
            CDataLayouts.schemaSetFormat(schema, formatString);
            assertThat(CDataLayouts.schemaFormat(schema).address()).isEqualTo(formatString.address());
        }
    }
}
