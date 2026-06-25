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
package io.tileverse.parquetry.stac;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DefaultParquetRecordBatch;
import io.tileverse.parquetry.batch.DoubleVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.batch.Validity;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Writes a minimal stac-geoparquet item-table for index-reader tests: one row per item with the item id, collection id,
 * a flat bbox (four doubles), and the data asset href. The full spec's nested bbox struct and assets map are flattened
 * here to keep the fixture small; the reader treats these columns as canonical.
 */
final class StacItemTableParquet {

    private StacItemTableParquet() {}

    record Row(String id, String collection, double xmin, double ymin, double xmax, double ymax, String assetHref) {}

    static Path write(Path file, List<Row> rows) throws Exception {
        ParquetSchema schema = itemTableSchema();
        WriteOptions options = WriteOptions.builder().tempDir(file.getParent()).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options);
                ParquetRecordBatch batch = batch(schema, rows)) {
            writer.writeBatch(batch);
        }
        return file;
    }

    private static ParquetRecordBatch batch(ParquetSchema schema, List<Row> rows) {
        int count = rows.size();
        Validity allValid = Validity.allValid(count);
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of("item_id"), utf8Column(rows, Row::id, allValid));
        columns.put(ColumnPath.of("asset_href"), utf8Column(rows, Row::assetHref, allValid));
        columns.put(ColumnPath.of("bbox_xmin"), doubleColumn(rows, Row::xmin, allValid));
        columns.put(ColumnPath.of("bbox_ymin"), doubleColumn(rows, Row::ymin, allValid));
        columns.put(ColumnPath.of("bbox_xmax"), doubleColumn(rows, Row::xmax, allValid));
        columns.put(ColumnPath.of("bbox_ymax"), doubleColumn(rows, Row::ymax, allValid));
        columns.put(ColumnPath.of("collection"), utf8Column(rows, Row::collection, allValid));
        return new DefaultParquetRecordBatch(schema, columns, count, Arena.ofShared());
    }

    private static ColumnVector utf8Column(
            List<Row> rows, java.util.function.Function<Row, String> field, Validity validity) {
        MemorySegment[] values = new MemorySegment[rows.size()];
        for (int i = 0; i < rows.size(); i++) {
            byte[] utf8 = field.apply(rows.get(i)).getBytes(StandardCharsets.UTF_8);
            values[i] = MemorySegment.ofArray(utf8);
        }
        return BinaryVector.materialized(values, validity);
    }

    private static ColumnVector doubleColumn(
            List<Row> rows, java.util.function.ToDoubleFunction<Row> field, Validity validity) {
        double[] values = new double[rows.size()];
        for (int i = 0; i < rows.size(); i++) {
            values[i] = field.applyAsDouble(rows.get(i));
        }
        return DoubleVector.materialized(values, validity);
    }

    private static ParquetSchema itemTableSchema() {
        List<SchemaNode> leaves = List.of(
                utf8Leaf("item_id"),
                utf8Leaf("asset_href"),
                doubleLeaf("bbox_xmin"),
                doubleLeaf("bbox_ymin"),
                doubleLeaf("bbox_xmax"),
                doubleLeaf("bbox_ymax"),
                utf8Leaf("collection"));
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, leaves, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive utf8Leaf(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive doubleLeaf(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.DOUBLE, OptionalInt.empty(), Optional.empty(), -1);
    }
}
