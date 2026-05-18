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
package io.tileverse.parquetry.assembly;

import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Row-by-row column accessor combining decoded values with rep/def levels.
 *
 * <p>The Dremel record assembler walks multiple ColumnReaders in lockstep; each reader exposes:
 *
 * <ul>
 *   <li>{@link #currentRepetitionLevel()} indicating where the next row starts (0 for top-level rows; higher values for
 *       nested repeated fields).
 *   <li>{@link #currentDefinitionLevel()} indicating null-ness and the level at which an optional ancestor became null.
 *       Equal to {@link #maxDefinitionLevel()} means the non-null value is materialized in {@link #currentValue()};
 *       less means null at some level.
 *   <li>{@link #currentValue()} the boxed primitive (or read-only ByteBuffer for binary), valid only when def level ==
 *       max def level.
 * </ul>
 *
 * <p>Lifecycle: positioned BEFORE the first row. Call {@link #hasNext()} to check whether the reader has more rows,
 * then read the levels/value, then {@link #consume()} to advance.
 *
 * <p>Thread-confined; one reader per column per row-group-reader instance.
 */
public interface ColumnReader {

    /** Whether there are more rows to read in this column. */
    boolean hasNext();

    /** The current row's repetition level (positioned but not yet consumed). */
    int currentRepetitionLevel();

    /** The current row's definition level (positioned but not yet consumed). */
    int currentDefinitionLevel();

    /**
     * The current row's value. Returns null if {@code currentDefinitionLevel() < maxDefinitionLevel()}. For binary
     * types returns a read-only {@code ByteBuffer}; for INT96 a read-only 12-byte buffer.
     */
    Object currentValue();

    /** Advance past the current row. After this call, the next row (if any) is positioned. */
    void consume();

    /** The schema path of this column. */
    ColumnPath columnPath();

    /** The maximum repetition level for this column (computed from the schema's ancestry). */
    int maxRepetitionLevel();

    /** The maximum definition level for this column. */
    int maxDefinitionLevel();
}
