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

/**
 * One column's worth of values for a {@link ParquetRecordBatch}. Vectors are always-materialized: the typed payload
 * (primitive arrays for primitives, a shared backing buffer for binary kinds) is set at construction. Per-page decode
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
                LevelListVector,
                LevelMapVector,
                StructVector,
                VariantVector {

    /** Logical row count this vector holds. */
    int size();

    /** The immutable per-row null mask. */
    Validity validity();

    /** Whether row {@code row} is null. */
    default boolean isNull(int row) {
        return validity().isNull(row);
    }

    /** Whether row {@code row} is non-null. */
    default boolean isValid(int row) {
        return validity().isValid(row);
    }

    /** Whether any row in this vector is null. */
    default boolean hasNulls() {
        return validity().hasNulls();
    }

    /** Number of null rows in this vector. */
    default int nullCount() {
        return validity().nullCount();
    }

    /**
     * Approximate heap bytes this vector's backing holds. Used as a soft budget signal, not an exact allocator: leaf
     * vectors count their typed backing plus validity; nested vectors add their children's bytes.
     */
    long approximateHeapBytes();

    /**
     * Type-agnostic accessor: the value at {@code row} as a boxed primitive (for the primitive leaves) or a reference
     * value such as a read-only {@link java.lang.foreign.MemorySegment} (for the binary kinds), or {@code null} on a
     * null row. The compiler inserts a {@code checkcast} at a concrete reference target: {@code Integer x = vec.get(r)}
     * on a long vector throws {@link ClassCastException} at the assignment. There is no such check at a
     * {@code var}/{@code Object} target. The boxing-free accessors are {@code getInt}/{@code getLong}/... and
     * {@code isNull}/{@code hasNulls}.
     *
     * <p>The nested {@link ListVector} / {@link MapVector} / {@link StructVector} materialize through the materializer
     * (which holds the schema context a sub-record or collection needs) and throw here.
     */
    default <T> T get(int row) {
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " materializes through the materializer rather than get");
    }

    /**
     * Returns a dictionary-free equivalent of this vector: a dictionary-encoded leaf expands its per-row indexes into a
     * consolidated backing, every other vector returns itself unchanged. Output formats that do not encode a dictionary
     * (Arrow IPC) consolidate first; the spill path keeps the dictionary form.
     */
    default ColumnVector toConsolidated() {
        return this;
    }
}
