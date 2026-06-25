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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.iceberg.IcebergPartitionSpec.PartitionField;

class IcebergPartitionSpecTest {

    private static final IcebergField CATEGORY = new IcebergField(2, "category", "string", true);
    private static final IcebergField TS = new IcebergField(3, "ts", "long", true);

    @Test
    void emptyWhenNoSpecHasFields() {
        IcebergPartitionSpec spec = IcebergPartitionSpec.of(List.of(), List.of(CATEGORY, TS));
        assertThat(spec.isEmpty()).isTrue();
        assertThat(spec.identitySourceFields()).isEmpty();
    }

    @Test
    void resolvesAnIdentityPartitionToItsSourceField() {
        IcebergPartitionSpec spec = IcebergPartitionSpec.of(
                List.of(new PartitionField(1000, 2, "category", "identity")), List.of(CATEGORY, TS));

        assertThat(spec.isEmpty()).isFalse();
        assertThat(spec.byPartitionFieldId(1000)).contains(new PartitionField(1000, 2, "category", "identity"));
        assertThat(spec.identitySourceFields()).containsOnlyKeys(2);
        assertThat(spec.identitySourceFields().get(2)).isEqualTo(CATEGORY);
    }

    @Test
    void transformPartitionIsNotAnIdentitySource() {
        IcebergPartitionSpec spec =
                IcebergPartitionSpec.of(List.of(new PartitionField(1000, 3, "ts_day", "day")), List.of(CATEGORY, TS));

        assertThat(spec.isEmpty()).isFalse();
        assertThat(spec.identitySourceFields()).isEmpty();
    }

    @Test
    void unionsFieldsAcrossEvolvedSpecs() {
        IcebergPartitionSpec spec = IcebergPartitionSpec.of(
                List.of(
                        new PartitionField(1000, 2, "category", "identity"),
                        new PartitionField(1001, 3, "ts", "identity")),
                List.of(CATEGORY, TS));

        assertThat(spec.byPartitionFieldId(1000)).isPresent();
        assertThat(spec.byPartitionFieldId(1001)).isPresent();
        assertThat(spec.identitySourceFields()).containsOnlyKeys(2, 3);
    }
}
