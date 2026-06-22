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
package io.tileverse.parquetry.data;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DefaultParquetRecordBatch;
import io.tileverse.parquetry.batch.LongVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.batch.Validity;
import io.tileverse.parquetry.data.WriteOptions.GeoParquetMetadataMode;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/** Writes small Parquet files through the columnar writer for {@link ParquetReaderFileStatsIT}. */
final class FileStatsFixtures {

    private FileStatsFixtures() {}

    /**
     * Writes a single-row-group file with one nullable INT64 column. A {@code null} entry in {@code values} is written
     * as a SQL null (no validity bit set), every other entry as its primitive value.
     */
    static Path writeIntColumn(Path dir, String column, Long[] values) throws Exception {
        ParquetSchema schema = flatSchema(optionalInt64(column));
        Path file = dir.resolve(column + ".parquet");
        try (ParquetWriter writer = ParquetWriter.create(
                        Files.newOutputStream(file),
                        schema,
                        WriteOptions.builder().tempDir(dir).build());
                ParquetRecordBatch batch = longBatch(schema, column, values)) {
            writer.writeBatch(batch);
        }
        return file;
    }

    private static ParquetRecordBatch longBatch(ParquetSchema schema, String column, Long[] values) {
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

    /**
     * Writes a GeoParquet file with one geometry column holding the given points (each a 2D {@code POINT(x y)} WKB),
     * with both the v1.1 {@code geo} key/value blob and v2.0 native geospatial statistics. The writer unions the
     * per-row-group geometry bounds into the file-level bounds the reader then exposes.
     */
    static Path writePoints(Path dir, String column, double[][] points) throws Exception {
        ParquetSchema schema = flatSchema(requiredBinary(column));
        WriteOptions options = WriteOptions.builder()
                .tempDir(dir)
                .geoParquetMetadata(GeoParquetMetadataMode.DUAL_V1_1_AND_V2_0)
                .crsEpsg(column, 4326)
                .build();
        Path file = dir.resolve(column + ".parquet");
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options);
                ParquetRecordBatch batch = pointBatch(schema, column, points)) {
            writer.writeBatch(batch);
        }
        return file;
    }

    private static ParquetRecordBatch pointBatch(ParquetSchema schema, String column, double[][] points) {
        MemorySegment[] wkb = new MemorySegment[points.length];
        for (int i = 0; i < points.length; i++) {
            wkb[i] = MemorySegment.ofArray(wkbPoint(points[i][0], points[i][1]));
        }
        BitSet allValid = new BitSet(points.length);
        allValid.set(0, points.length);
        Validity validity = Validity.of(allValid, points.length);
        Map<ColumnPath, ColumnVector> columns = Map.of(ColumnPath.of(column), BinaryVector.materialized(wkb, validity));
        return new DefaultParquetRecordBatch(schema, columns, points.length, Arena.ofShared());
    }

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children =
                Stream.of(leaves).map(SchemaNode.class::cast).toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive optionalInt64(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive requiredBinary(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static byte[] wkbPoint(double x, double y) {
        ByteBuffer buffer = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1);
        buffer.putInt(1);
        buffer.putDouble(x);
        buffer.putDouble(y);
        return buffer.array();
    }
}
