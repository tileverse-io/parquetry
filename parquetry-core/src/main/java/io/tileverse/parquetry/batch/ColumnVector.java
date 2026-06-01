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
                StructVector,
                VariantVector {

    /** Logical row count this vector carries. */
    int size();

    /** Validity mask: bit i is set iff row i is non-null. */
    BitSet validity();

    /**
     * Approximate heap bytes this vector's backing holds. Used as a soft budget signal, not an exact allocator: leaf
     * vectors count their typed backing plus validity; nested vectors add their children's bytes.
     */
    long approximateHeapBytes();

    /** Approximate heap cost of a validity bitmap covering {@code rowCount} rows. */
    static long validityBytes(int rowCount) {
        return (long) rowCount / Byte.SIZE + 1;
    }

    /**
     * Returns the value at {@code row} as a boxed object, or {@code null} when the validity bit is clear. A leaf vector
     * returns a boxed primitive, or a read-only {@link java.lang.foreign.MemorySegment} for the binary and INT96 kinds;
     * a null row yields {@code null} even where a primitive backing array parks a default such as {@code 0}.
     *
     * <p>The nested {@link ListVector} / {@link MapVector} / {@link StructVector} do not implement this. A nested cell
     * materializes through the materializer, which holds the schema context a sub-record or collection needs, and the
     * nested vectors throw {@link UnsupportedOperationException} here.
     */
    default Object getOrNull(int row) {
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " materializes through the materializer rather than getOrNull");
    }
}
