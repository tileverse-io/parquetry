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
package io.tileverse.parquetry.internal.write;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

/** Builds a one-batch {@link ParquetRecordBatch} from rows expressed as per-leaf value maps, for write tests. */
public final class WriteFixtures {

    private WriteFixtures() {}

    public static ParquetRecordBatch batch(ParquetSchema schema, List<Map<ColumnPath, Object>> rows) {
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);
        for (Map<ColumnPath, Object> row : rows) {
            appendRow(builder, schema, row);
        }
        return builder.build();
    }

    /**
     * Stages one row's cells onto an existing builder and closes the row. Lets writer-bound appenders reuse the same
     * value-to-setter dispatch as {@link #batch} while keeping heap bounded for streaming producers.
     */
    public static void appendRow(ParquetRecordBatchBuilder builder, ParquetSchema schema, Map<ColumnPath, Object> row) {
        List<ColumnPath> leaves = schema.leafColumns();
        for (int i = 0; i < leaves.size(); i++) {
            setCell(builder, i, row.get(leaves.get(i)));
        }
        builder.endRow();
    }

    private static void setCell(ParquetRecordBatchBuilder builder, int col, Object value) {
        switch (value) {
            case null -> builder.setNull(col);
            case Boolean booleanValue -> builder.setBoolean(col, booleanValue);
            case Integer intValue -> builder.setInt(col, intValue);
            case Long longValue -> builder.setLong(col, longValue);
            case Float floatValue -> builder.setFloat(col, floatValue);
            case Double doubleValue -> builder.setDouble(col, doubleValue);
            case String stringValue -> builder.setString(col, stringValue);
            case UUID uuidValue -> builder.setUuid(col, uuidValue);
            case MemorySegment segmentValue -> builder.setBinary(col, segmentValue);
            case byte[] bytesValue -> builder.setBinary(col, MemorySegment.ofArray(bytesValue));
            default -> throw new IllegalArgumentException("Unsupported test value type: " + value.getClass());
        }
    }
}
