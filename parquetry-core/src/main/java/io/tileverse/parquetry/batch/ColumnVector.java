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
package io.tileverse.parquetry.batch;

import java.util.BitSet;

/**
 * One column's worth of values for a {@link ParquetRecordBatch}. Vectors are always-materialized: the typed payload
 * (primitive arrays for primitives, {@code MemorySegment[]} for binary kinds) is set at construction. Per-page decode
 * happens once in {@code BatchColumnReader.loadNextPage}; each {@code readBatch} call slices the decoded page state
 * into a fresh vector.
 *
 * @see ParquetRecordBatch
 */
public sealed interface ColumnVector
        permits IntVector,
                LongVector,
                FloatVector,
                DoubleVector,
                BooleanVector,
                BinaryVector,
                FixedLenBinaryVector,
                Int96Vector,
                ListVector,
                MapVector,
                StructVector {

    /** Logical row count this vector carries. */
    int size();

    /** Validity mask: bit i is set iff row i is non-null. */
    BitSet validity();
}
