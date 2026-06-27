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
package io.tileverse.parquetry.internal.variant;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.variant.VariantMetadata;

class VariantMetadataCorpusTest {

    @Test
    void readsObjectPrimitiveCorpusKeys() {
        VariantMetadata metadata = new VariantMetadata(MemorySegment.ofArray(VariantCorpus.metadata("object_primitive"))
                .asReadOnly());
        assertThat(metadata.dictionarySize()).as("7 keys").isEqualTo(7);
        assertThat(metadata.idOf("int_field")).as("int_field id").isZero();
        assertThat(metadata.key(6)).as("last key").isEqualTo("timestamp_field");
        assertThat(metadata.key(metadata.idOf("string_field")))
                .as("round-trip key")
                .isEqualTo("string_field");
    }
}
