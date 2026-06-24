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
import java.util.stream.Stream;

import io.tileverse.parquetry.arrow.ipc.ArrowIpcWriter;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.FilesetDataset;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Streams the rows of a dataset to an Arrow IPC stream, choosing between the columnar fast path and the record path.
 *
 * <p>When neither a row filter nor a limit is in play, the columnar batches the reader already produces flow straight
 * into the encoder. Any filter or limit forces the record path, which decodes rows, applies the limit, then repacks
 * them into batches; this keeps record-level filtering exact and lets {@code limit} short-circuit the read.
 *
 * <p>{@link ArrowIpcWriter#write} validates {@code projectedSchema} before draining any batch, hence an unsupported
 * column (nested, INT96, DECIMAL) throws before a single row is read; no pre-validation is needed here.
 */
public final class ArrowOutput {

    private static final int TARGET_BATCH_ROWS = 8192;

    private ArrowOutput() {}

    public static void write(
            ParquetDataset dataset,
            ParquetSchema projectedSchema,
            Optional<GeoParquetMetadata> geo,
            ArrowOutputRequest request,
            OutputStream out) {
        if (canFastPath(request)) {
            try (Stream<ParquetRecordBatch> batches =
                    dataset.readBatches(Predicate.ALWAYS_TRUE, request.projection(), ReadOptions.DEFAULTS)) {
                ArrowIpcWriter.write(projectedSchema, geo, batches, out);
            }
            return;
        }
        try (Stream<ParquetRecord> records =
                dataset.read(request.predicate(), request.projection(), ReadOptions.DEFAULTS)) {
            writeRecordPath(records, projectedSchema, geo, request, out);
        }
    }

    public static void write(
            FilesetDataset dataset,
            ParquetSchema projectedSchema,
            Optional<GeoParquetMetadata> geo,
            ArrowOutputRequest request,
            OutputStream out) {
        if (canFastPath(request)) {
            try (Stream<ParquetRecordBatch> batches =
                    dataset.readBatches(Predicate.ALWAYS_TRUE, request.projection(), ReadOptions.DEFAULTS)) {
                ArrowIpcWriter.write(projectedSchema, geo, batches, out);
            }
            return;
        }
        try (Stream<ParquetRecord> records =
                dataset.read(request.predicate(), request.projection(), ReadOptions.DEFAULTS)) {
            writeRecordPath(records, projectedSchema, geo, request, out);
        }
    }

    private static boolean canFastPath(ArrowOutputRequest request) {
        return !request.hasFilter() && request.limit() == Long.MAX_VALUE;
    }

    private static void writeRecordPath(
            Stream<ParquetRecord> records,
            ParquetSchema projectedSchema,
            Optional<GeoParquetMetadata> geo,
            ArrowOutputRequest request,
            OutputStream out) {
        long limit = request.limit();
        Stream<ParquetRecord> limited = (limit == Long.MAX_VALUE) ? records : records.limit(limit);
        Stream<ParquetRecordBatch> batches = RecordBatchPacker.pack(limited, projectedSchema, TARGET_BATCH_ROWS);
        ArrowIpcWriter.write(projectedSchema, geo, batches, out);
    }
}
