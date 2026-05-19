/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

class SchemaElementTest {

    @Test
    void leafElementCarriesPrimitiveType() {
        SchemaElement leaf = new SchemaElement(
                Optional.of(PhysicalType.INT32),
                OptionalInt.empty(),
                Optional.of(FieldRepetitionType.OPTIONAL),
                "year",
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.of(7));
        assertThat(leaf.type()).contains(PhysicalType.INT32);
        assertThat(leaf.name()).isEqualTo("year");
        assertThat(leaf.fieldId()).hasValue(7);
    }

    @Test
    void groupElementHasNumChildren() {
        SchemaElement group = new SchemaElement(
                Optional.empty(),
                OptionalInt.empty(),
                Optional.of(FieldRepetitionType.REQUIRED),
                "address",
                OptionalInt.of(3),
                Optional.empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty());
        assertThat(group.type()).isEmpty();
        assertThat(group.numChildren()).hasValue(3);
    }

    @Test
    void encodingStatsCarriesCounts() {
        EncodingStats stats = new EncodingStats(PageType.DATA_PAGE, Encoding.PLAIN, 12);
        assertThat(stats.pageType()).isEqualTo(PageType.DATA_PAGE);
        assertThat(stats.encoding()).isEqualTo(Encoding.PLAIN);
        assertThat(stats.count()).isEqualTo(12);
    }
}
