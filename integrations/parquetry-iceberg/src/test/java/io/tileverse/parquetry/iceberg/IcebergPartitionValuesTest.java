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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.iceberg.IcebergPartitionSpec.PartitionField;

class IcebergPartitionValuesTest {

    private static IcebergPartitionSpec spec(IcebergField source, String transform) {
        return IcebergPartitionSpec.of(
                List.of(new PartitionField(1000, source.fieldId(), source.name(), transform)), List.of(source));
    }

    @Test
    void convertsAStringIdentityValue() {
        IcebergPartitionSpec spec = spec(new IcebergField(2, "category", "string", true), "identity");
        Map<Integer, Value> constants = IcebergPartitionValues.constantsFor(spec, Map.of(1000, "a"));
        assertThat(constants).containsEntry(2, new Value.StringVal("a"));
    }

    @Test
    void convertsADateIdentityValueFromALocalDate() {
        IcebergPartitionSpec spec = spec(new IcebergField(2, "d", "date", true), "identity");
        LocalDate date = LocalDate.ofEpochDay(19000);
        Map<Integer, Value> constants = IcebergPartitionValues.constantsFor(spec, Map.of(1000, date));
        assertThat(constants).containsEntry(2, new Value.DateVal(date));
    }

    @Test
    void convertsADateIdentityValueFromRawEpochDays() {
        IcebergPartitionSpec spec = spec(new IcebergField(2, "d", "date", true), "identity");
        Map<Integer, Value> constants = IcebergPartitionValues.constantsFor(spec, Map.of(1000, 19000));
        assertThat(constants).containsEntry(2, new Value.DateVal(LocalDate.ofEpochDay(19000)));
    }

    @Test
    void convertsIntAndLongIdentityValues() {
        IcebergPartitionSpec ints = spec(new IcebergField(2, "i", "int", true), "identity");
        assertThat(IcebergPartitionValues.constantsFor(ints, Map.of(1000, 7))).containsEntry(2, new Value.IntVal(7));

        IcebergPartitionSpec longs = spec(new IcebergField(2, "l", "long", true), "identity");
        assertThat(IcebergPartitionValues.constantsFor(longs, Map.of(1000, 7L)))
                .containsEntry(2, new Value.LongVal(7L));
    }

    @Test
    void skipsTransformPartitions() {
        IcebergPartitionSpec spec = spec(new IcebergField(2, "ts", "long", true), "day");
        assertThat(IcebergPartitionValues.constantsFor(spec, Map.of(1000, 19000)))
                .isEmpty();
    }

    @Test
    void skipsANullPartitionValue() {
        IcebergPartitionSpec spec = spec(new IcebergField(2, "category", "string", true), "identity");
        assertThat(IcebergPartitionValues.constantsFor(spec, Collections.singletonMap(1000, null)))
                .isEmpty();
    }

    @Test
    void skipsAnIdentityPartitionWhoseSourceColumnWasDropped() {
        IcebergPartitionSpec spec =
                IcebergPartitionSpec.of(List.of(new PartitionField(1000, 2, "category", "identity")), List.of());
        assertThat(IcebergPartitionValues.constantsFor(spec, Map.of(1000, "a"))).isEmpty();
    }

    @Test
    void failsFastOnAnUnsupportedIdentitySourceType() {
        IcebergPartitionSpec spec = spec(new IcebergField(2, "blob", "binary", true), "identity");
        Map<Integer, Object> tuple = Map.of(1000, "x");
        assertThatThrownBy(() -> IcebergPartitionValues.constantsFor(spec, tuple))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("blob");
    }
}
