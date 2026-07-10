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

import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Writes single-column flat {@code INT64} Parquet files through the parquetry columnar writer, the shared fixture for
 * catalog tests that exercise footer statistics. The writer emits the per-column min/max/null-count statistics those
 * tests rely on for whole-file pruning.
 */
public final class FlatLongParquet {

    private FlatLongParquet() {}

    /**
     * Writes {@code values} as a flat, nullable {@code INT64} single-column Parquet file at {@code file}. Every value
     * is non-null; the validity buffer marks them all present.
     *
     * @return {@code file}, for caller convenience
     */
    public static Path writeIntFile(Path file, String column, long[] values) throws Exception {
        Long[] boxed = new Long[values.length];
        for (int i = 0; i < values.length; i++) {
            boxed[i] = values[i];
        }
        return writeNullableIntFile(file, column, boxed);
    }

    /**
     * Writes {@code values} as a flat, nullable {@code INT64} single-column Parquet file at {@code file}. A
     * {@code null} entry is written as a SQL null (its validity bit stays unset), every other entry as its primitive
     * value. An all-null column produces an absent min/max and a footer null-count equal to the row count.
     *
     * @return {@code file}, for caller convenience
     */
    public static Path writeNullableIntFile(Path file, String column, Long[] values) throws Exception {
        ParquetSchema schema = flatInt64Schema(column);
        WriteOptions options = WriteOptions.builder().tempDir(file.getParent()).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options);
                ParquetRecordBatch batch = nullableLongBatch(schema, column, values)) {
            writer.writeBatch(batch);
        }
        return file;
    }

    /** A run of {@code length} consecutive longs starting at {@code start}. */
    public static long[] longRange(long start, int length) {
        long[] values = new long[length];
        for (int i = 0; i < length; i++) {
            values[i] = start + i;
        }
        return values;
    }

    private static ParquetRecordBatch nullableLongBatch(ParquetSchema schema, String column, Long[] values) {
        int rows = values.length;
        long[] longs = new long[rows];
        BitSet validBits = new BitSet(rows);
        for (int i = 0; i < rows; i++) {
            if (values[i] != null) {
                longs[i] = values[i];
                validBits.set(i);
            }
        }
        Validity validity = Validity.of(validBits, rows);
        Map<ColumnPath, ColumnVector> columns = Map.of(ColumnPath.of(column), LongVector.materialized(longs, validity));
        return new DefaultParquetRecordBatch(schema, columns, rows, Arena.ofShared());
    }

    private static ParquetSchema flatInt64Schema(String column) {
        SchemaNode.Primitive leaf = new SchemaNode.Primitive(
                column, Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(leaf), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
