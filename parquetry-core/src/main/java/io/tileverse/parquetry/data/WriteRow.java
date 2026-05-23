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
package io.tileverse.parquetry.data;

import java.lang.foreign.MemorySegment;
import java.util.Map;

import io.tileverse.parquetry.data.write.ColumnChunkWriter;
import io.tileverse.parquetry.data.write.RowGroupWriter;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Read access to one record's leaf values, keyed by column path.
 *
 * <p>Mirror of {@link io.tileverse.parquetry.materializer.RowAccessor} on the write side. The {@link RowGroupWriter}
 * pulls one value per leaf column from this carrier, then dispatches it to the corresponding {@link ColumnChunkWriter}
 * based on the column's {@link io.tileverse.parquetry.schema.PrimitiveKind}.
 *
 * <p>Returned values follow the same boxing convention as {@link io.tileverse.parquetry.materializer.RowAccessor}:
 * {@code Integer}, {@code Long}, {@code Float}, {@code Double}, {@code Boolean} for primitives, a read-only
 * {@link MemorySegment} for BYTE_ARRAY / FIXED_LEN_BYTE_ARRAY / INT96 columns, and {@code null} for absent leaves on
 * non-required columns.
 */
public interface WriteRow {

    /**
     * Returns the value for {@code path}, or {@code null} when the leaf is absent and the column allows nulls.
     *
     * @throws ParquetWriteException when {@code path} is missing on a required column or carries a value whose Java
     *     class does not match the column's primitive kind
     */
    Object value(ColumnPath path);

    /** Adapts a {@link Map} keyed by column path into a {@link WriteRow}. */
    static WriteRow of(Map<ColumnPath, Object> values) {
        Map<ColumnPath, Object> copy = Map.copyOf(values);
        return copy::get;
    }
}
