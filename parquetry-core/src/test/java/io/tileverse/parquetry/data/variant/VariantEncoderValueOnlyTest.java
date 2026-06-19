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
package io.tileverse.parquetry.data.variant;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.ParquetFormatException;

class VariantEncoderValueOnlyTest {

    @Test
    void encodesObjectAgainstExistingMetadataFieldIds() {
        VariantEncoder.Encoded reference = new VariantEncoder()
                .startObject()
                .field("a")
                .addInt(1)
                .field("b")
                .addInt(2)
                .endObject()
                .encode();
        VariantMetadata metadata = new VariantMetadata(reference.metadata());

        MemorySegment valueOnly = new VariantEncoder()
                .startObject()
                .field("a")
                .addInt(1)
                .field("b")
                .addInt(2)
                .endObject()
                .encodeValue(metadata);

        assertThat(valueOnly.toArray(JAVA_BYTE)).isEqualTo(reference.value().toArray(JAVA_BYTE));
    }

    @Test
    void splicesAPreEncodedValueVerbatim() {
        VariantEncoder.Encoded scalar = new VariantEncoder().addInt(42).encode();
        VariantMetadata empty = new VariantMetadata(scalar.metadata());

        MemorySegment spliced = new VariantEncoder().addEncoded(scalar.value()).encodeValue(empty);

        assertThat(spliced.toArray(JAVA_BYTE)).isEqualTo(scalar.value().toArray(JAVA_BYTE));
    }

    @Test
    void rejectsFieldNameAbsentFromMetadata() {
        VariantMetadata empty =
                new VariantMetadata(new VariantEncoder().addInt(0).encode().metadata());
        VariantEncoder encoder =
                new VariantEncoder().startObject().field("missing").addInt(1).endObject();
        assertThatThrownBy(() -> encoder.encodeValue(empty))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("missing");
    }
}
