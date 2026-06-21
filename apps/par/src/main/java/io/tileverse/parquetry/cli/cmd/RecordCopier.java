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
package io.tileverse.parquetry.cli.cmd;

import java.lang.foreign.MemorySegment;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.tileverse.parquetry.cli.UnsupportedSchemaException;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Copies flat {@link ParquetRecord}s into a writer's {@link ParquetRecordBatchBuilder appender}.
 *
 * <p>This is the canonical record-to-appender copy path. The per-leaf metadata a copy needs (the primitive kind to
 * dispatch on, and where each appender column reads from in the record) is loop-invariant across the rows of one copy.
 * {@link #forSchema(ParquetSchema)} precomputes it once into an indexable plan; {@link #copyInto} then walks that plan
 * with integer-addressed setters and getters, allocating nothing per row and hashing no {@link ColumnPath} per cell.
 *
 * <p>The up-front {@link #requireWritable(ParquetSchema)} guard stays static: it runs once, before any copy, and gives
 * the user a clean error for an unsupported schema instead of a deep write failure mid-copy.
 */
public final class RecordCopier {

    private final PrimitiveKind[] kinds;
    private final ColumnPath[] leaves;
    private int[] recordColumn;

    private RecordCopier(PrimitiveKind[] kinds, ColumnPath[] leaves) {
        this.kinds = kinds;
        this.leaves = leaves;
    }

    /**
     * Verifies the writer can reproduce {@code schema}: top-level fields must be non-repeated primitives, and no leaf
     * may be INT96 or a Variant logical type. Throws {@link UnsupportedSchemaException} otherwise.
     */
    public static void requireWritable(ParquetSchema schema) {
        for (SchemaNode child : schema.root().children()) {
            if (!(child instanceof SchemaNode.Primitive prim)) {
                throw new UnsupportedSchemaException(
                        "cp supports flat primitive schemas only; column '" + child.name() + "' is a group");
            }
            if (prim.repetition() == Repetition.REPEATED) {
                throw new UnsupportedSchemaException(
                        "cp supports flat primitive schemas only; column '" + prim.name() + "' is repeated");
            }
            if (prim.kind() == PrimitiveKind.INT96) {
                throw new UnsupportedSchemaException("cp cannot write INT96 column '" + prim.name() + "'");
            }
            if (prim.logicalType()
                    .filter(lt -> lt instanceof LogicalType.Variant)
                    .isPresent()) {
                throw new UnsupportedSchemaException("cp cannot write Variant column '" + prim.name() + "'");
            }
        }
    }

    /**
     * Precomputes the copy plan for {@code writeSchema}: appender column {@code i} corresponds to the {@code i}-th
     * leaf, and its primitive kind selects the integer-indexed setter to use. The record-column lookup is resolved
     * lazily on the first record (see {@link #copyInto}), where the record's own column layout is known.
     */
    public static RecordCopier forSchema(ParquetSchema writeSchema) {
        List<ColumnPath> leafColumns = writeSchema.leafColumns();
        int leafCount = leafColumns.size();
        PrimitiveKind[] kinds = new PrimitiveKind[leafCount];
        ColumnPath[] leaves = new ColumnPath[leafCount];
        for (int i = 0; i < leafCount; i++) {
            ColumnPath leaf = leafColumns.get(i);
            SchemaNode.Primitive primitive =
                    (SchemaNode.Primitive) writeSchema.find(leaf).orElseThrow();
            kinds[i] = primitive.kind();
            leaves[i] = leaf;
        }
        return new RecordCopier(kinds, leaves);
    }

    /**
     * Copies one record's leaf values into {@code appender} and closes the appended row. Callers must pre-validate the
     * schema with {@link #requireWritable(ParquetSchema)}.
     */
    public void copyInto(ParquetRecordBatchBuilder appender, ParquetRecord record) {
        int[] columns = recordColumns(record);
        for (int i = 0; i < kinds.length; i++) {
            int recordCol = columns[i];
            copyCell(appender, record, i, recordCol);
        }
        appender.endRow();
    }

    private void copyCell(ParquetRecordBatchBuilder appender, ParquetRecord record, int appenderCol, int recordCol) {
        if (record.isNull(recordCol)) {
            appender.setNull(appenderCol);
            return;
        }
        switch (kinds[appenderCol]) {
            case BOOLEAN -> appender.setBoolean(appenderCol, record.getBoolean(recordCol));
            case INT32 -> appender.setInt(appenderCol, record.getInt(recordCol));
            case INT64 -> appender.setLong(appenderCol, record.getLong(recordCol));
            case FLOAT -> appender.setFloat(appenderCol, record.getFloat(recordCol));
            case DOUBLE -> appender.setDouble(appenderCol, record.getDouble(recordCol));
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY ->
                appender.setBinary(appenderCol, (MemorySegment) record.get(recordCol));
            case INT96 ->
                throw new UnsupportedSchemaException(
                        "cp cannot write INT96 column '" + leaves[appenderCol].dot() + "'");
        }
    }

    /**
     * Resolves, once, the record column each appender column reads from. The record materialized from the same
     * projected schema the writer is built over lays its leaves out in the same order, but resolving the mapping by
     * path keeps the copy correct even when it does not. The result is cached and reused for every subsequent record of
     * the copy.
     */
    private int[] recordColumns(ParquetRecord record) {
        if (recordColumn == null) {
            recordColumn = resolveRecordColumns(record);
        }
        return recordColumn;
    }

    private int[] resolveRecordColumns(ParquetRecord record) {
        Map<ColumnPath, Integer> columnByPath = indexRecordColumns(record);
        int[] columns = new int[leaves.length];
        for (int i = 0; i < leaves.length; i++) {
            Integer recordCol = columnByPath.get(leaves[i]);
            if (recordCol == null) {
                throw new UnsupportedSchemaException(
                        "cp cannot find source column '" + leaves[i].dot() + "' in the record");
            }
            columns[i] = recordCol;
        }
        return columns;
    }

    private Map<ColumnPath, Integer> indexRecordColumns(ParquetRecord record) {
        int columnCount = record.columnCount();
        Map<ColumnPath, Integer> columnByPath = new HashMap<>(columnCount);
        for (int col = 0; col < columnCount; col++) {
            columnByPath.put(record.columnPath(col), col);
        }
        return columnByPath;
    }
}
