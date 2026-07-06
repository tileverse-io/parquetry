/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.iceberg.IcebergFileSchema.FileColumn;
import io.tileverse.parquetry.iceberg.IcebergManifests.DeleteFileRef;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Reads an Iceberg equality-delete file with parquetry's own reader. An equality-delete file is a Parquet file whose
 * columns are the equality-id fields; each row is a tuple of values to delete. This reads those columns by their
 * physical path in the delete file, located by field id (id-less delete files resolve through the table's name
 * mapping), and decodes each cell into a {@link Value} by the equality field's Iceberg type, preserving a null cell as
 * a null tuple element so the anti-predicate can apply null-safe matching.
 */
final class IcebergEqualityDeletes {

    private IcebergEqualityDeletes() {}

    /**
     * The parsed contents of one equality-delete file: the table columns the tuples filter on, and one tuple per delete
     * row. Each tuple holds one {@link Value} (or null) per column, aligned to {@link #columns()} in order.
     */
    record Tuples(List<ColumnPath> columns, List<List<Value>> rows) {
        Tuples {
            columns = List.copyOf(columns);
            rows = List.copyOf(rows);
        }
    }

    /**
     * Reads {@code delete} into its delete tuples, resolving each equality field id to its table column and physical
     * delete-file column through {@code schema}.
     */
    static Tuples read(IcebergFileIO io, DeleteFileRef delete, IcebergSchema schema, IcebergNameMapping nameMapping) {
        try (ByteRangeSource source = io.open(delete.location())) {
            ParquetSource deletes = ParquetSource.open(source);
            List<EqualityColumn> columns =
                    resolveColumns(delete, schema, IcebergFileSchema.of(deletes.schema(), nameMapping));
            return new Tuples(tableColumns(columns), collectTuples(deletes, columns));
        }
    }

    private static List<EqualityColumn> resolveColumns(
            DeleteFileRef delete, IcebergSchema schema, IcebergFileSchema fileSchema) {
        List<EqualityColumn> columns = new ArrayList<>(delete.equalityFieldIds().size());
        for (int fieldId : delete.equalityFieldIds()) {
            columns.add(resolveColumn(fieldId, schema, fileSchema));
        }
        return columns;
    }

    private static EqualityColumn resolveColumn(int fieldId, IcebergSchema schema, IcebergFileSchema fileSchema) {
        IcebergField field = schema.fieldById(fieldId)
                .orElseThrow(() -> new IcebergFormatException(
                        "equality delete references field id " + fieldId + " absent from the table schema"));
        FileColumn fileColumn = fileSchema
                .byFieldId(fieldId)
                .orElseThrow(
                        () -> new IcebergFormatException("equality delete file has no column for field id " + fieldId));
        return new EqualityColumn(ColumnPath.of(field.name()), field.type(), fileColumn.path());
    }

    private static List<ColumnPath> tableColumns(List<EqualityColumn> columns) {
        List<ColumnPath> tableColumns = new ArrayList<>(columns.size());
        for (EqualityColumn column : columns) {
            tableColumns.add(column.tableColumn());
        }
        return tableColumns;
    }

    private static List<List<Value>> collectTuples(ParquetSource deletes, List<EqualityColumn> columns) {
        Projection projection = Projection.ofPhysical(physicalPaths(columns));
        List<List<Value>> tuples = new ArrayList<>();
        try (Stream<ParquetRecord> rows = deletes.read(Predicate.ALWAYS_TRUE, projection, ReadOptions.DEFAULTS)) {
            rows.forEach(row -> tuples.add(tupleOf(row, columns)));
        }
        return tuples;
    }

    private static List<ColumnPath> physicalPaths(List<EqualityColumn> columns) {
        List<ColumnPath> paths = new ArrayList<>(columns.size());
        for (EqualityColumn column : columns) {
            paths.add(column.physicalPath());
        }
        return paths;
    }

    private static List<Value> tupleOf(ParquetRecord row, List<EqualityColumn> columns) {
        List<Value> tuple = new ArrayList<>(columns.size());
        for (EqualityColumn column : columns) {
            tuple.add(valueOf(row, column));
        }
        return tuple;
    }

    private static Value valueOf(ParquetRecord row, EqualityColumn column) {
        ColumnPath path = column.physicalPath();
        if (row.isNull(path)) {
            return null;
        }
        return decode(row, path, column.icebergType());
    }

    private static Value decode(ParquetRecord row, ColumnPath path, String icebergType) {
        return switch (icebergType) {
            case "int" -> new Value.IntVal(row.getInt(path));
            case "long" -> new Value.LongVal(row.getLong(path));
            case "float" -> new Value.FloatVal(row.getFloat(path));
            case "double" -> new Value.DoubleVal(row.getDouble(path));
            case "boolean" -> new Value.BoolVal(row.getBoolean(path));
            case "date" -> new Value.DateVal(LocalDate.ofEpochDay(row.getInt(path)));
            case "string" -> new Value.StringVal(row.getString(path));
            default ->
                throw new IcebergFormatException("cannot read equality delete column %s of unsupported type %s"
                        .formatted(path.dot(), icebergType));
        };
    }

    /**
     * One equality field to read from a delete file: the table column it maps to, the field's Iceberg type, and the
     * field's physical path within the delete file.
     */
    private record EqualityColumn(ColumnPath tableColumn, String icebergType, ColumnPath physicalPath) {}
}
