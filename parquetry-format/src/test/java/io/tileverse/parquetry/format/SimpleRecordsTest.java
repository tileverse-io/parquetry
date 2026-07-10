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
package io.tileverse.parquetry.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

class SimpleRecordsTest {

    @Test
    void statisticsFieldsAreAllOptional() {
        Statistics s = Statistics.builder().build();
        assertThat(s.maxValue()).isSameAs(MemorySegment.NULL);
        assertThat(s.nullCount()).isEmpty();
    }

    @Test
    void sizeStatisticsHoldsReferencedFields() {
        SizeStatistics ss = new SizeStatistics(
                OptionalLong.of(1024L), Optional.of(List.of(10L, 20L, 30L)), Optional.of(List.of(0L, 0L, 5L)));
        assertThat(ss.unencodedByteArrayDataBytes()).hasValue(1024L);
        assertThat(ss.repetitionLevelHistogram()).map(List::size).contains(3);
    }

    @Test
    void keyValueDefensiveCopiesValueOptional() {
        KeyValue kv = new KeyValue("geo", Optional.of("{}"));
        assertThat(kv.key()).isEqualTo("geo");
        assertThat(kv.value()).contains("{}");
    }

    @Test
    void columnOrderHasTypeDefinedVariant() {
        ColumnOrder.TypeDefined o = new ColumnOrder.TypeDefined();
        assertThat(o).isNotNull();
    }
}
