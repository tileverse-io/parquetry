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
 * One batch of rows from a Parquet read. Holds one {@link ColumnVector} per projected leaf. Vectors are heap-backed at
 * construction; their values remain valid even after {@link #close()}.
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

    /** Releases the batch's token Arena. Idempotent. Vectors remain accessible after close. */
    @Override
    void close();
}
