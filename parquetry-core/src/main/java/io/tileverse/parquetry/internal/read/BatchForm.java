/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.internal.read;

/**
 * The shape a row group's batches take as they leave {@link BatchRowGroupReader#nextBatch()}. The choice is internal
 * and picked per public read entry; it never reaches {@code ReadOptions}.
 *
 * <p>Both forms keep flat and row-aligned struct leaves addressable at the same column paths; they differ only in how a
 * row-aligned LIST or MAP group is wrapped, which is why a flat-column predicate evaluates identically over either.
 *
 * <p>Spill is independent of this choice: a batch that does not fit the decode budget is always serialized in the
 * assembled encoding. The spill codec bridges a level vector to its Arrow form at encode time, hence a {@link #LEVELS}
 * batch spills and restores with no special handling.
 */
public enum BatchForm {

    /**
     * A row-aligned LIST or MAP group becomes a level vector that keeps the dense leaf vectors plus batch-owned level
     * windows and defers the Dremel assembly to a lazy row view. A filtered streaming row read
     * ({@code ParquetFileReader.read} with a record-level filter) and {@code count} use this form: lazy navigation lets
     * the read skip assembling the cells of rows the filter discards, and {@code count} never materializes a row at
     * all.
     */
    LEVELS,

    /**
     * A row-aligned LIST or MAP group becomes the eager offset-and-child Arrow-shape vector. The public
     * {@code ParquetFileReader.readBatches} contract returns this form, the shape batch consumers expect, and an
     * unfiltered streaming row read uses it too: prepaying the assembly on idle decode workers reads back faster than
     * lazy navigation on the consumer's critical path when no rows are discarded.
     */
    ASSEMBLED
}
