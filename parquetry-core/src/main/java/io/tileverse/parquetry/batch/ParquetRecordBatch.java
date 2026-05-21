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

import java.util.Map;

import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * One batch of rows from a Parquet read. Holds one {@link ColumnVector} per projected leaf plus an
 * {@link java.lang.foreign.Arena} that owns all decompressed page memory. {@link #close()} closes the Arena,
 * invalidating any {@link java.lang.foreign.MemorySegment} views the vectors exposed.
 */
public sealed interface ParquetRecordBatch extends AutoCloseable permits DefaultParquetRecordBatch {

    /** The projected schema this batch's columns correspond to. */
    ParquetSchema projectedSchema();

    /** Row count for this batch (1 &lt;= rowCount &lt;= the natural page boundary cap). */
    int rowCount();

    /** Per-leaf vector lookup, keyed by column path. */
    Map<ColumnPath, ColumnVector> columns();

    /**
     * Adapts row {@code rowIndex} of this batch to a single {@link ParquetRecord}. The returned record is a view: it
     * holds a reference to this batch and reads through to the vectors on each accessor call. The view remains valid as
     * long as the producing batch (and its underlying page Arenas) does.
     */
    ParquetRecord materialize(int rowIndex);

    /**
     * Releases the batch's own Arena (if any allocations landed in it). Idempotent.
     *
     * <p><strong>MemorySegment validity:</strong> vectors and any {@link java.lang.foreign.MemorySegment} views they
     * expose remain valid as long as the producing {@link io.tileverse.parquetry.read.BatchRowGroupReader
     * BatchRowGroupReader}'s underlying column readers retain the page that backs them. Page Arenas are owned by
     * per-column readers and live until either the column reader advances past the page (the next call to
     * {@code readBatch} on that column triggers the prior page's release) or the {@code BatchRowGroupReader} is closed.
     * Consumers who need a value to outlive that window must copy it out before then (for example via
     * {@link ColumnVector#materialize()} or {@code segment.toArray(JAVA_BYTE)}).
     */
    @Override
    void close();
}
