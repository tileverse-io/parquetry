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
package io.tileverse.parquetry.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.format.ColumnOrder;
import io.tileverse.parquetry.testkit.TestCorpus;
import io.tileverse.parquetry.testsupport.ReadFixtures;

/**
 * Pins the column orders decoded from the footers of the {@code apache/parquet-testing} fixtures written with the
 * orders added to the format after the type-defined one.
 */
class FooterColumnOrderIT {

    private static final String DATA = "parquet-testing/data/";

    @TempDir
    Path tempDir;

    @Test
    void int96TimestampOrderDecodesAsTheInt96Order() {
        Map<String, ColumnOrder> orders = columnOrdersByLeaf("int96_timestamp_order.parquet");

        assertThat(orders).containsExactly(Map.entry("ts", new ColumnOrder.Int96TimestampOrder()));
    }

    @Test
    void floatingOrdersDecodeAsTotalOrderOrTypeDefinedPerColumn() {
        Map<String, ColumnOrder> orders = columnOrdersByLeaf("floating_orders_nan_count.parquet");

        ColumnOrder totalOrder = new ColumnOrder.Ieee754TotalOrder();
        ColumnOrder typeDefined = new ColumnOrder.TypeDefined();
        assertThat(orders)
                .containsOnly(
                        Map.entry("float_ieee754", totalOrder),
                        Map.entry("double_ieee754", totalOrder),
                        Map.entry("float16_ieee754", totalOrder),
                        Map.entry("float_typedef", typeDefined),
                        Map.entry("double_typedef", typeDefined),
                        Map.entry("float16_typedef", typeDefined));
    }

    /** The column orders of the corpus fixture {@code fileName}, keyed by leaf column name. */
    private Map<String, ColumnOrder> columnOrdersByLeaf(String fileName) {
        Path file = TestCorpus.extractFile(DATA + fileName, tempDir);
        return ReadFixtures.columnOrdersByLeafName(file);
    }
}
