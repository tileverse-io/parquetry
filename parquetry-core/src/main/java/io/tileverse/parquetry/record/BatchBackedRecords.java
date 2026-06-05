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
package io.tileverse.parquetry.record;

import java.util.Map;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.BooleanVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DoubleVector;
import io.tileverse.parquetry.batch.FixedLenBinaryVector;
import io.tileverse.parquetry.batch.FloatVector;
import io.tileverse.parquetry.batch.Int96Vector;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.ListVector;
import io.tileverse.parquetry.batch.LongVector;
import io.tileverse.parquetry.batch.MapVector;
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.batch.VariantVector;
import io.tileverse.parquetry.materializer.ListMaterializer;
import io.tileverse.parquetry.materializer.MapMaterializer;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * Shared dispatch helper for the {@link io.tileverse.parquetry.materializer.RowAccessor} adapters over a batch
 * ({@link BatchRowAccessor} and {@link StructRowAccessor}). Each views a single row of a {@code Map<ColumnPath,
 * ColumnVector>}; {@link #get} dispatches on the vector type to box the cell value in one place to keep the adapters
 * thin.
 */
final class BatchBackedRecords {

    private BatchBackedRecords() {}

    /**
     * Returns the boxed value at row {@code rowIndex} in {@code vec}, or {@code null} when the vector is missing or the
     * row's validity bit is clear. For {@link StructVector} cells, returns a {@link DefaultParquetRecord} over a
     * {@link StructRowAccessor} for the nested row, letting callers drill down through {@link ParquetRecord} accessors.
     */
    static Object get(Map<ColumnPath, ColumnVector> columns, int rowIndex, ColumnPath col, ParquetSchema schema) {
        ColumnVector vec = columns.get(col);
        if (vec == null || vec.isNull(rowIndex)) {
            return null;
        }
        return switch (vec) {
            case IntVector iv -> iv.getInt(rowIndex);
            case LongVector lv -> lv.getLong(rowIndex);
            case FloatVector fv -> fv.getFloat(rowIndex);
            case DoubleVector dv -> dv.getDouble(rowIndex);
            case BooleanVector bv -> bv.getBoolean(rowIndex);
            case BinaryVector bv -> bv.get(rowIndex);
            case FixedLenBinaryVector fb -> fb.get(rowIndex);
            case Int96Vector iv -> iv.get(rowIndex);
            case ListVector list -> ListMaterializer.materializeAt(list, rowIndex, schema);
            case MapVector map -> MapMaterializer.materializeAt(map, rowIndex, schema);
            case StructVector struct ->
                new DefaultParquetRecord(schema, new StructRowAccessor(struct, rowIndex, schema));
            case VariantVector variant -> variant.get(rowIndex);
        };
    }
}
