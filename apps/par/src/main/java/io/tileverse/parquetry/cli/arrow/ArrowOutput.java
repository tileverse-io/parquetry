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
package io.tileverse.parquetry.cli.arrow;

import java.io.OutputStream;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import io.tileverse.parquetry.arrow.ipc.ArrowIpcWriter;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetReader;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Streams the rows of a dataset to an Arrow IPC stream through the columnar read path. The reader's batches flow
 * straight into the encoder, which densifies any filtered batch at the export boundary; {@code limit} is applied here
 * as a batch-level cap that passes whole batches through and slices the one batch straddling the limit.
 *
 * <p>{@link ArrowIpcWriter#write} validates {@code projectedSchema} before draining any batch, hence an unsupported
 * column throws before a single row is read; no pre-validation is needed here.
 */
public final class ArrowOutput {

    private ArrowOutput() {}

    public static void write(
            ParquetReader dataset,
            ParquetSchema projectedSchema,
            Optional<GeoParquetMetadata> geo,
            ArrowOutputRequest request,
            OutputStream out) {
        try (Stream<ParquetRecordBatch> batches =
                dataset.readBatches(request.predicate(), request.projection(), ReadOptions.DEFAULTS)) {
            ArrowIpcWriter.write(projectedSchema, geo, cappedAt(batches, request.limit()), out);
        }
    }

    /**
     * Caps {@code batches} at {@code limit} total rows: whole batches pass through until the limit, the batch
     * straddling it is sliced to the remainder, and the pull stops there, short-circuiting the read the way the text
     * path's {@code takeWhile} does. A batch pulled past the cap is closed before the stream ends; an emitted slice
     * owns its source batch, and closing the slice downstream releases both.
     */
    private static Stream<ParquetRecordBatch> cappedAt(Stream<ParquetRecordBatch> batches, long limit) {
        if (limit == Long.MAX_VALUE) {
            return batches;
        }
        AtomicLong remaining = new AtomicLong(limit);
        return batches.takeWhile(batch -> {
                    if (remaining.get() > 0) {
                        return true;
                    }
                    batch.close();
                    return false;
                })
                .map(batch -> {
                    long rest = remaining.get();
                    if (batch.rowCount() <= rest) {
                        remaining.addAndGet(-batch.rowCount());
                        return batch;
                    }
                    remaining.set(0);
                    return batch.slice(0, (int) rest);
                });
    }
}
