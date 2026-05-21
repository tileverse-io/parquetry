/*
 * Copyright (c) 2026 Tileverse.io
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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;

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
import io.tileverse.parquetry.materializer.ListMaterializer;
import io.tileverse.parquetry.materializer.MapMaterializer;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Shared dispatch helpers for {@link BatchBackedParquetRecord} and {@link BatchBackedSubRecord}. Both classes view a
 * single row of a {@code Map<ColumnPath, ColumnVector>}; the helpers here implement the typed accessor surface in one
 * place so the two record classes stay thin.
 */
final class BatchBackedRecords {

    private BatchBackedRecords() {}

    /**
     * Returns the boxed value at row {@code rowIndex} in {@code vec}, or {@code null} when the vector is missing or the
     * row's validity bit is clear. For {@link StructVector} cells, returns a {@link BatchBackedSubRecord} so callers
     * can drill down through {@link ParquetRecord} accessors.
     */
    static Object get(Map<ColumnPath, ColumnVector> columns, int rowIndex, ColumnPath col, ParquetSchema schema) {
        ColumnVector vec = columns.get(col);
        if (vec == null || !vec.validity().get(rowIndex)) {
            return null;
        }
        return switch (vec) {
            case IntVector iv -> iv.get(rowIndex);
            case LongVector lv -> lv.get(rowIndex);
            case FloatVector fv -> fv.get(rowIndex);
            case DoubleVector dv -> dv.get(rowIndex);
            case BooleanVector bv -> bv.get(rowIndex);
            case BinaryVector bv -> bv.get(rowIndex);
            case FixedLenBinaryVector fb -> fb.get(rowIndex);
            case Int96Vector iv -> iv.get(rowIndex);
            case ListVector list -> ListMaterializer.materializeAt(list, rowIndex);
            case MapVector map -> MapMaterializer.materializeAt(map, rowIndex);
            case StructVector struct -> new BatchBackedSubRecord(struct, rowIndex, schema);
        };
    }

    static boolean getBoolean(
            Map<ColumnPath, ColumnVector> columns, int rowIndex, ColumnPath col, ParquetSchema schema) {
        requireKind(schema, col, PrimitiveKind.BOOLEAN, "getBoolean");
        BooleanVector vec = (BooleanVector) requireVector(columns, col);
        return vec.get(rowIndex);
    }

    static int getInt(Map<ColumnPath, ColumnVector> columns, int rowIndex, ColumnPath col, ParquetSchema schema) {
        requireKind(schema, col, PrimitiveKind.INT32, "getInt");
        IntVector vec = (IntVector) requireVector(columns, col);
        return vec.get(rowIndex);
    }

    static long getLong(Map<ColumnPath, ColumnVector> columns, int rowIndex, ColumnPath col, ParquetSchema schema) {
        requireKind(schema, col, PrimitiveKind.INT64, "getLong");
        LongVector vec = (LongVector) requireVector(columns, col);
        return vec.get(rowIndex);
    }

    static float getFloat(Map<ColumnPath, ColumnVector> columns, int rowIndex, ColumnPath col, ParquetSchema schema) {
        requireKind(schema, col, PrimitiveKind.FLOAT, "getFloat");
        FloatVector vec = (FloatVector) requireVector(columns, col);
        return vec.get(rowIndex);
    }

    static double getDouble(Map<ColumnPath, ColumnVector> columns, int rowIndex, ColumnPath col, ParquetSchema schema) {
        requireKind(schema, col, PrimitiveKind.DOUBLE, "getDouble");
        DoubleVector vec = (DoubleVector) requireVector(columns, col);
        return vec.get(rowIndex);
    }

    static byte[] getBinary(Map<ColumnPath, ColumnVector> columns, int rowIndex, ColumnPath col, ParquetSchema schema) {
        requireBinaryKind(schema, col, "getBinary");
        return copyBytes(columns, col, rowIndex);
    }

    static String getString(Map<ColumnPath, ColumnVector> columns, int rowIndex, ColumnPath col, ParquetSchema schema) {
        requireBinaryKind(schema, col, "getString");
        return new String(copyBytes(columns, col, rowIndex), StandardCharsets.UTF_8);
    }

    static boolean isNull(Map<ColumnPath, ColumnVector> columns, int rowIndex, ColumnPath col) {
        ColumnVector vec = columns.get(col);
        return vec == null || !vec.validity().get(rowIndex);
    }

    private static byte[] copyBytes(Map<ColumnPath, ColumnVector> columns, ColumnPath col, int rowIndex) {
        ColumnVector vec = requireVector(columns, col);
        MemorySegment segment =
                switch (vec) {
                    case BinaryVector bv -> bv.get(rowIndex);
                    case FixedLenBinaryVector fb -> fb.get(rowIndex);
                    case Int96Vector iv -> iv.get(rowIndex);
                    default ->
                        throw new IllegalStateException("Column " + col.dot() + " has non-binary vector type "
                                + vec.getClass().getSimpleName());
                };
        return segment.toArray(JAVA_BYTE);
    }

    private static ColumnVector requireVector(Map<ColumnPath, ColumnVector> columns, ColumnPath col) {
        ColumnVector vec = columns.get(col);
        if (vec == null) {
            throw new IllegalArgumentException("Column " + col.dot() + " is not present in this record");
        }
        return vec;
    }

    private static void requireKind(ParquetSchema schema, ColumnPath col, PrimitiveKind expected, String accessor) {
        PrimitiveKind actual = lookupKind(schema, col, accessor);
        if (actual != expected) {
            throw mismatch(col, actual, accessor);
        }
    }

    private static void requireBinaryKind(ParquetSchema schema, ColumnPath col, String accessor) {
        PrimitiveKind actual = lookupKind(schema, col, accessor);
        if (actual != PrimitiveKind.BYTE_ARRAY && actual != PrimitiveKind.FIXED_LEN_BYTE_ARRAY) {
            throw mismatch(col, actual, accessor);
        }
    }

    private static PrimitiveKind lookupKind(ParquetSchema schema, ColumnPath col, String accessor) {
        Optional<Field> field = schema.find(col);
        if (field.isEmpty()) {
            throw new IllegalArgumentException(
                    "Column " + col.dot() + " is not present in the projected schema (accessor " + accessor + ")");
        }
        if (!(field.get() instanceof Field.Primitive primitive)) {
            throw new IllegalArgumentException(
                    "Column " + col.dot() + " is a group column; accessor " + accessor + " requires a primitive leaf");
        }
        return primitive.kind();
    }

    private static IllegalArgumentException mismatch(ColumnPath col, PrimitiveKind actual, String accessor) {
        return new IllegalArgumentException("Column " + col.dot() + " is " + actual + "; requested " + accessor);
    }
}
