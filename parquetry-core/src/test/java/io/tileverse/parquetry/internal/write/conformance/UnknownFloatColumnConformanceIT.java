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
package io.tileverse.parquetry.internal.write.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.FloatColumnOrder;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * parquet-java opens a file written in IEEE 754 total order holding FLOAT and DOUBLE columns annotated UNKNOWN, the
 * annotation of a column of nulls of an undetermined type. It accepts total order for a FLOAT or DOUBLE column without
 * annotation and for FLOAT16, and rejects the footer of a file declaring that order for a column with another
 * annotation: such columns keep the type-defined order.
 */
@Tag("conformance")
class UnknownFloatColumnConformanceIT {

    private static final String ID = "id";
    private static final String MEASURE = "measure";
    private static final String UNKNOWN_FLOAT = "f";
    private static final String UNKNOWN_DOUBLE = "d";
    private static final int ROWS = 3;

    @TempDir
    Path tempDir;

    private Path file;

    @BeforeEach
    void writeNullsInTheUnknownColumns() throws IOException {
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .floatColumnOrder(FloatColumnOrder.IEEE_754_TOTAL_ORDER)
                .build();
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (int id = 0; id < ROWS; id++) {
            rows.add(Map.of(ColumnPath.of(ID), id, ColumnPath.of(MEASURE), id * 0.5));
        }
        file = WriteFixtures.writeRows(tempDir.resolve("unknown-floats.parquet"), schema(), options, rows);
    }

    @Test
    void parquetJavaOpensTheFileAndSeesTheTypeDefinedOrderOnTheUnknownColumns() throws IOException {
        MessageType schema = WriteConformanceSupport.readFooterViaParquetJava(file)
                .getFileMetaData()
                .getSchema();

        for (String column : List.of(UNKNOWN_FLOAT, UNKNOWN_DOUBLE)) {
            assertThat(schema.getType(column).asPrimitiveType().columnOrder())
                    .as("column order of %s", column)
                    .isEqualTo(ColumnOrder.typeDefined());
        }
        assertThat(schema.getType(MEASURE).asPrimitiveType().columnOrder())
                .as("column order of the DOUBLE column without annotation")
                .isEqualTo(ColumnOrder.ieee754TotalOrder());
    }

    @Test
    void parquetJavaRejectsTotalOrderOnAFloatColumnAnnotatedUnknown() {
        Types.PrimitiveBuilder<PrimitiveType> unknownFloatInTotalOrder = Types.optional(PrimitiveTypeName.FLOAT)
                .as(LogicalTypeAnnotation.unknownType())
                .columnOrder(ColumnOrder.ieee754TotalOrder());

        assertThatThrownBy(() -> unknownFloatInTotalOrder.named(UNKNOWN_FLOAT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("IEEE_754_TOTAL_ORDER");
    }

    @Test
    void parquetJavaReadsTheNullsOfTheUnknownColumns() throws IOException {
        List<GenericRecord> rows = WriteConformanceSupport.readWithAvro(file);

        assertThat(rows).hasSize(ROWS);
        for (GenericRecord row : rows) {
            assertThat(row.get(UNKNOWN_FLOAT)).isNull();
            assertThat(row.get(UNKNOWN_DOUBLE)).isNull();
        }
    }

    private static ParquetSchema schema() {
        LogicalType unknown = new LogicalType.UnknownType();
        OptionalInt none = OptionalInt.empty();
        return WriteFixtures.schemaOf(
                WriteFixtures.requiredLeaf(ID, PrimitiveKind.INT32),
                WriteFixtures.requiredLeaf(MEASURE, PrimitiveKind.DOUBLE),
                WriteFixtures.optionalLeaf(UNKNOWN_FLOAT, PrimitiveKind.FLOAT, none, unknown),
                WriteFixtures.optionalLeaf(UNKNOWN_DOUBLE, PrimitiveKind.DOUBLE, none, unknown));
    }
}
