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

import java.util.Map;

import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * One batch of rows from a Parquet read. Holds one {@link ColumnVector} per projected leaf. Most vectors are
 * heap-backed and their values remain valid after {@link #close()}. Segment-backed vectors are decoded into off-heap
 * buffers this batch owns (registered via {@link #registerBuffer(AutoCloseable)}); those buffers are released by
 * {@link #close()} and their values must not be read afterward.
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
     * holds a reference to this batch and reads through to the vectors on each accessor call.
     */
    ParquetRecord materialize(int rowIndex);

    /** Approximate heap bytes this batch's vectors hold; a soft budget signal, not an exact figure. */
    long approximateHeapBytes();

    /**
     * Registers an action to run once when this batch is closed, after its Arena is released. Used to return a decode
     * budget reservation when the consumer is done with the batch. At most one action is registered per batch.
     */
    void attachReleaseAction(Runnable releaseAction);

    /**
     * Registers an off-heap buffer this batch owns; the batch closes it when the batch closes. A segment-backed vector
     * reads through such a buffer, which must outlive the vector's accessors and is released exactly once on
     * {@link #close()}.
     */
    void registerBuffer(AutoCloseable buffer);

    /**
     * Releases the batch's owned off-heap buffers and its Arena. Idempotent. Heap-backed vector values remain readable
     * after close; segment-backed vector values do not.
     */
    @Override
    void close();
}
