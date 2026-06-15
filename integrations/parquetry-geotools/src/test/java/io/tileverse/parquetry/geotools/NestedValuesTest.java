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
package io.tileverse.parquetry.geotools;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.geotools.data.nested.NestedType;
import org.geotools.data.nested.Struct;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.catalog.ParquetDatasetCatalog;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Verifies that {@link NestedValues#translate} copies a live nested value into an owned, batch-independent form: the
 * result is JDK collections holding {@link Struct}/scalars (no {@code ParquetRecord}) and stays readable after the
 * source dataset closes.
 */
class NestedValuesTest {

    private static final ColumnPath ADDRESSES = ColumnPath.of("addresses");

    @Test
    void translatesAddressesAndSurvivesSourceClose(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("nested.parquet");
        NestedFixtures.writeSample(file);

        List<Object> translatedPerRow = new ArrayList<>();
        try (ParquetDatasetCatalog catalog = NestedFixtures.openCatalog(file)) {
            ParquetDataset dataset = catalog.dataset("nested");
            NestedType addressesType = addressesType(dataset);
            try (Stream<ParquetRecord> rows = dataset.read()) {
                rows.forEachOrdered(
                        row -> translatedPerRow.add(NestedValues.translate(row.get(ADDRESSES), addressesType)));
            }
        }

        assertThat(translatedPerRow).hasSize(3);
        assertFirstRowAddresses(translatedPerRow.get(0));
        assertEmptyAddresses(translatedPerRow.get(1));
        assertNullLocalityAddress(translatedPerRow.get(2));
    }

    private void assertFirstRowAddresses(Object value) {
        assertThat(value).isInstanceOf(List.class);
        List<?> addresses = (List<?>) value;
        assertThat(addresses).hasSize(2);

        Object first = addresses.get(0);
        assertThat(first).isInstanceOf(Map.class).isInstanceOf(Struct.class);
        Map<?, ?> firstAddress = (Map<?, ?>) first;
        assertThat(firstAddress.get("locality")).isEqualTo("NYC");

        assertNoParquetRecord(value);
    }

    private void assertEmptyAddresses(Object value) {
        assertThat(value).isInstanceOf(List.class);
        assertThat((List<?>) value).isEmpty();
    }

    private void assertNullLocalityAddress(Object value) {
        assertThat(value).isInstanceOf(List.class);
        List<?> addresses = (List<?>) value;
        assertThat(addresses).hasSize(2);

        Object second = addresses.get(1);
        assertThat(second).isInstanceOf(Struct.class);
        Map<?, ?> secondAddress = (Map<?, ?>) second;
        assertThat(secondAddress.get("locality")).isNull();
    }

    private void assertNoParquetRecord(Object value) {
        assertThat(value).isNotInstanceOf(ParquetRecord.class);
        if (value instanceof List<?> list) {
            list.forEach(this::assertNoParquetRecord);
        } else if (value instanceof Map<?, ?> map) {
            map.values().forEach(this::assertNoParquetRecord);
        }
    }

    private NestedType addressesType(ParquetDataset dataset) {
        SchemaNode node = dataset.schema().root().children().stream()
                .filter(child -> child.name().equals("addresses"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no 'addresses' column"));
        return NestedTypes.of(node);
    }
}
