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
 * One column's worth of values for a {@link ParquetRecordBatch}. Two-state ADT: a freshly produced vector holds the
 * decompressed page bytes in a raw {@code MemorySegment} (no decode work done); {@link #materialize()} transitions to
 * the typed carrier (primitive arrays for primitives, {@code MemorySegment[]} for binary). Columns the consumer never
 * reads stay raw and are freed at batch close.
 *
 * <p>{@link #materializeSurvivors(BitSet)} is the hook for the future vectorized row-level predicate evaluator. It
 * decodes only the rows whose bit is set; no production caller uses it yet.
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

    /** Returns true once {@link #materialize()} has populated the typed carrier. */
    boolean isMaterialized();

    /** Forces the raw -> materialized transition. Idempotent. */
    void materialize();

    /**
     * Decodes only the rows whose bit is set in {@code survivors}. Idempotent; a later call with a wider survivor set
     * re-decodes the newly-included rows. Reserved for a future {@code VectorPredicateEvaluator}; no production code
     * calls it yet.
     */
    void materializeSurvivors(BitSet survivors);
}
