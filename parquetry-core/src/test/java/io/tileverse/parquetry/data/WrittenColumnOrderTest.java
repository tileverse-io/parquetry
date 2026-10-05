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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.tileverse.parquetry.data.WriteOptions.FloatColumnOrder;
import io.tileverse.parquetry.data.WriteOptions.ParquetVersion;
import io.tileverse.parquetry.format.ColumnOrder;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.ReadFixtures;

/**
 * The footer of a written file declares the order of the statistics of each leaf column. FLOAT, DOUBLE and FLOAT16
 * columns declare the type-defined order unless the write asks for IEEE 754 total order; the other columns declare the
 * type-defined order either way.
 */
class WrittenColumnOrderTest {

    private static final ColumnOrder TOTAL_ORDER = new ColumnOrder.Ieee754TotalOrder();
    private static final ColumnOrder TYPE_DEFINED = new ColumnOrder.TypeDefined();

    @TempDir
    Path tempDir;

    @ParameterizedTest
    @EnumSource(ParquetVersion.class)
    void columnsDeclareTheTypeDefinedOrderByDefault(ParquetVersion version) throws IOException {
        WriteOptions options =
                WriteOptions.builder().tempDir(tempDir).parquetVersion(version).build();
        Path file = write(flatSchema(), flatRow(), options);

        Map<String, ColumnOrder> orders = ReadFixtures.columnOrdersByLeafName(file);

        assertThat(orders.keySet()).containsExactlyInAnyOrder("id", "ratio", "measure", "half", "name", "amount");
        assertThat(orders.values()).containsOnly(TYPE_DEFINED);
    }

    @ParameterizedTest
    @EnumSource(ParquetVersion.class)
    void floatingPointColumnsDeclareTotalOrderOnRequest(ParquetVersion version) throws IOException {
        WriteOptions options = totalOrder().parquetVersion(version).build();
        Path file = write(flatSchema(), flatRow(), options);

        Map<String, ColumnOrder> orders = ReadFixtures.columnOrdersByLeafName(file);

        assertThat(orders)
                .containsOnly(
                        Map.entry("id", TYPE_DEFINED),
                        Map.entry("ratio", TOTAL_ORDER),
                        Map.entry("measure", TOTAL_ORDER),
                        Map.entry("half", TOTAL_ORDER),
                        Map.entry("name", TYPE_DEFINED),
                        Map.entry("amount", TYPE_DEFINED));
    }

    @Test
    void nestedFloatingPointLeafDeclaresTotalOrderOnRequest() throws IOException {
        SchemaNode point = new SchemaNode.Group(
                "point",
                Repetition.REQUIRED,
                List.of(
                        WriteFixtures.requiredLeaf("label", PrimitiveKind.BYTE_ARRAY),
                        WriteFixtures.requiredLeaf("x", PrimitiveKind.DOUBLE)),
                Optional.empty(),
                -1);
        Map<ColumnPath, Object> row = new HashMap<>();
        row.put(ColumnPath.of("point", "label"), "origin");
        row.put(ColumnPath.of("point", "x"), 0.0);
        ParquetSchema schema = WriteFixtures.schemaOf(point);
        WriteOptions options = totalOrder().build();
        Path file = write(schema, row, options);

        Map<String, ColumnOrder> orders = ReadFixtures.columnOrdersByLeafName(file);

        assertThat(orders).containsOnly(Map.entry("label", TYPE_DEFINED), Map.entry("x", TOTAL_ORDER));
    }

    @Test
    void floatingPointColumnAnnotatedUnknownDeclaresTheTypeDefinedOrder() throws IOException {
        LogicalType unknown = new LogicalType.UnknownType();
        OptionalInt none = OptionalInt.empty();
        ParquetSchema schema = WriteFixtures.schemaOf(
                WriteFixtures.requiredLeaf("id", PrimitiveKind.INT32),
                WriteFixtures.optionalLeaf("ratio", PrimitiveKind.FLOAT, none, unknown),
                WriteFixtures.optionalLeaf("measure", PrimitiveKind.DOUBLE, none, unknown));
        Map<ColumnPath, Object> row = Map.of(ColumnPath.of("id"), 1);
        WriteOptions options = totalOrder().build();
        Path file = write(schema, row, options);

        Map<String, ColumnOrder> orders = ReadFixtures.columnOrdersByLeafName(file);

        assertThat(orders)
                .containsOnly(
                        Map.entry("id", TYPE_DEFINED),
                        Map.entry("ratio", TYPE_DEFINED),
                        Map.entry("measure", TYPE_DEFINED));
    }

    private WriteOptions.Builder totalOrder() {
        return WriteOptions.builder().tempDir(tempDir).floatColumnOrder(FloatColumnOrder.IEEE_754_TOTAL_ORDER);
    }

    private Path write(ParquetSchema schema, Map<ColumnPath, Object> row, WriteOptions options) throws IOException {
        Path file = Files.createTempFile(tempDir, "orders", ".parquet");
        return WriteFixtures.writeRows(file, schema, options, List.of(row));
    }

    private static Map<ColumnPath, Object> flatRow() {
        Map<ColumnPath, Object> row = new HashMap<>();
        row.put(ColumnPath.of("id"), 1);
        row.put(ColumnPath.of("ratio"), 0.5f);
        row.put(ColumnPath.of("measure"), 2.5);
        row.put(ColumnPath.of("half"), WriteFixtures.halfFloatBytes(Float.floatToFloat16(1.0f)));
        row.put(ColumnPath.of("name"), "one");
        row.put(ColumnPath.of("amount"), new byte[] {0x00, 0x00, 0x00, 0x07});
        return row;
    }

    private static ParquetSchema flatSchema() {
        OptionalInt none = OptionalInt.empty();
        OptionalInt halfFloatBytes = OptionalInt.of(WriteFixtures.HALF_FLOAT_BYTES);
        OptionalInt decimalBytes = OptionalInt.of(Integer.BYTES);
        return WriteFixtures.schemaOf(
                WriteFixtures.requiredLeaf("id", PrimitiveKind.INT32),
                WriteFixtures.requiredLeaf("ratio", PrimitiveKind.FLOAT),
                WriteFixtures.requiredLeaf("measure", PrimitiveKind.DOUBLE),
                WriteFixtures.requiredLeaf(
                        "half", PrimitiveKind.FIXED_LEN_BYTE_ARRAY, halfFloatBytes, new LogicalType.Float16Type()),
                WriteFixtures.requiredLeaf("name", PrimitiveKind.BYTE_ARRAY, none, new LogicalType.StringType()),
                WriteFixtures.requiredLeaf(
                        "amount", PrimitiveKind.FIXED_LEN_BYTE_ARRAY, decimalBytes, new LogicalType.Decimal(2, 9)));
    }
}
