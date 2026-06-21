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
package io.tileverse.parquetry.dataset;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Value;

class PartitionTypeInferenceTest {

    @Test
    void infersLong() {
        assertThat(PartitionTypeInference.infer("2024")).isEqualTo(new Value.LongVal(2024L));
    }

    @Test
    void infersDouble() {
        assertThat(PartitionTypeInference.infer("3.14")).isEqualTo(new Value.DoubleVal(3.14));
    }

    @Test
    void infersDate() {
        assertThat(PartitionTypeInference.infer("2024-01-15"))
                .isEqualTo(new Value.DateVal(LocalDate.parse("2024-01-15")));
    }

    @Test
    void leadingZeroStaysString() {
        assertThat(PartitionTypeInference.infer("08")).isEqualTo(new Value.StringVal("08"));
        assertThat(PartitionTypeInference.infer("007")).isEqualTo(new Value.StringVal("007"));
    }

    @Test
    void zeroItselfIsLong() {
        assertThat(PartitionTypeInference.infer("0")).isEqualTo(new Value.LongVal(0L));
    }

    @Test
    void overflowFallsThroughToDoubleOrString() {
        assertThat(PartitionTypeInference.infer("99999999999999999999999")).isInstanceOf(Value.DoubleVal.class);
    }

    @Test
    void nonNumericStaysString() {
        assertThat(PartitionTypeInference.infer("buildings")).isEqualTo(new Value.StringVal("buildings"));
    }

    @Test
    void consistentColumnKeepsType() {
        assertThat(PartitionTypeInference.inferConsistent(List.of("2023", "2024")))
                .isEqualTo(PartitionKind.LONG);
    }

    @Test
    void inconsistentColumnDegradesToString() {
        assertThat(PartitionTypeInference.inferConsistent(List.of("2024", "spring")))
                .isEqualTo(PartitionKind.STRING);
    }
}
