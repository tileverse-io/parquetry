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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.DoubleVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Pins the writer's fail-loud contract when a bbox covering is active: the covering is derived from the geometry
 * column's WKB, hence every batch must actually provide that column as binary. A batch that does not gets an error
 * naming the column, rather than a cast failure deep inside derivation.
 */
class ParquetFileWriterCoveringGuardTest {

    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");
    private static final int ROW_COUNT = 4;

    @TempDir
    Path tempDir;

    @Test
    void batchWithoutTheGeometryColumnIsRejected() throws Exception {
        ParquetSchema schema = geometryOnlySchema();

        try (ParquetFileWriter writer = coveringWriter(schema);
                ParquetRecordBatch batch = batchWithColumns(schema, Map.of())) {

            assertThatThrownBy(() -> writer.writeBatch(batch))
                    .isInstanceOf(ParquetWriteException.class)
                    .hasMessageContaining("geometry")
                    .hasMessageContaining("this batch has none");
        }
    }

    @Test
    void batchWhoseGeometryColumnIsNotBinaryIsRejected() throws Exception {
        ParquetSchema schema = geometryOnlySchema();
        Map<ColumnPath, ColumnVector> notWkb =
                Map.of(GEOMETRY, DoubleVector.materialized(new double[ROW_COUNT], Validity.allValid(ROW_COUNT)));

        try (ParquetFileWriter writer = coveringWriter(schema);
                ParquetRecordBatch batch = batchWithColumns(schema, notWkb)) {

            assertThatThrownBy(() -> writer.writeBatch(batch))
                    .isInstanceOf(ParquetWriteException.class)
                    .hasMessageContaining("geometry")
                    .hasMessageContaining("binary WKB");
        }
    }

    /** A writer over a WGS84 geometry column with defaults, which resolves to an active FLOAT covering. */
    private ParquetFileWriter coveringWriter(ParquetSchema schema) throws Exception {
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .crsEpsg("geometry", 4326)
                .build();
        Path file = tempDir.resolve("covering-guard.parquet");
        return ParquetFileWriter.create(Files.newOutputStream(file), schema, options);
    }

    private static ParquetRecordBatch batchWithColumns(ParquetSchema schema, Map<ColumnPath, ColumnVector> columns) {
        return DefaultParquetRecordBatch.ofHeap(schema, columns, ROW_COUNT);
    }

    private static ParquetSchema geometryOnlySchema() {
        SchemaNode.Group root = new SchemaNode.Group(
                "schema", Repetition.REQUIRED, List.of(requiredBinary("geometry")), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredBinary(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }
}
