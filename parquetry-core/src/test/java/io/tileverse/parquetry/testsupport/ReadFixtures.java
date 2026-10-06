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
package io.tileverse.parquetry.testsupport;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.format.ColumnOrder;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.SchemaElement;
import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * Fixtures for read tests: the options of a read without metadata pruning, the comparison of a pruned count with an
 * unpruned one, and the column orders declared by a footer.
 */
public final class ReadFixtures {

    /** Read options leaving each row to the record-level filter: no metadata tier prunes. */
    public static final ReadOptions METADATA_PRUNING_OFF = ReadOptions.builder()
            .useStatsFilter(false)
            .useDictionaryFilter(false)
            .useColumnIndexFilter(false)
            .useBloomFilter(false)
            .build();

    private ReadFixtures() {}

    /**
     * Asserts that {@code predicate} counts {@code expected} rows of {@code file}, with the metadata pruning tiers on
     * and off.
     */
    public static void assertCountWithAndWithoutPruning(Path file, Predicate predicate, long expected) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            assertThat(reader.count(predicate, ReadOptions.DEFAULTS))
                    .as("rows matching %s", predicate)
                    .isEqualTo(reader.count(predicate, METADATA_PRUNING_OFF))
                    .isEqualTo(expected);
        }
    }

    /** The column orders in the footer of {@code file}, keyed by the name of the leaf column at the same position. */
    public static Map<String, ColumnOrder> columnOrdersByLeafName(Path file) {
        FileMetaData footer;
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            footer = ParquetFormat.readFooter(source);
        }
        List<String> leaves = leafNames(footer.schema());
        List<ColumnOrder> orders = footer.columnOrders().orElseThrow();
        assertThat(orders).as("one column order per leaf").hasSameSizeAs(leaves);
        Map<String, ColumnOrder> byLeaf = new HashMap<>();
        for (int leaf = 0; leaf < leaves.size(); leaf++) {
            byLeaf.put(leaves.get(leaf), orders.get(leaf));
        }
        return byLeaf;
    }

    private static List<String> leafNames(List<SchemaElement> schema) {
        return schema.stream()
                .filter(element -> element.numChildren().isEmpty())
                .map(SchemaElement::name)
                .toList();
    }
}
