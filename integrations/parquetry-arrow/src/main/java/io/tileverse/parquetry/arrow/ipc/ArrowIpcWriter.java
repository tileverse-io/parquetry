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
package io.tileverse.parquetry.arrow.ipc;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import io.tileverse.parquetry.arrow.ipc.LogicalColumns.LogicalColumn;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Writes an Arrow IPC stream (streaming format) for a sequence of {@link ParquetRecordBatch}: a Schema message, one
 * RecordBatch message per batch, then end-of-stream. The projected schema is validated up front; an unsupported column
 * fails before any byte is written.
 *
 * <p>The {@link OutputStream} overload flushes the stream after writing the complete IPC stream, but does not close it,
 * ensuring a buffered sink (or a process that exits without flushing) does not drop the trailing bytes. The
 * {@link WritableByteChannel} overload neither flushes nor closes the channel; the caller owns it.
 */
public final class ArrowIpcWriter {

    private ArrowIpcWriter() {}

    public static void write(
            ParquetSchema projectedSchema,
            Optional<GeoParquetMetadata> geo,
            Stream<ParquetRecordBatch> batches,
            OutputStream out) {
        write(projectedSchema, geo, batches, Channels.newChannel(out));
        try {
            out.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("failed flushing Arrow IPC stream", e);
        }
    }

    public static void write(
            ParquetSchema projectedSchema,
            Optional<GeoParquetMetadata> geo,
            Stream<ParquetRecordBatch> batches,
            WritableByteChannel channel) {
        GeoArrowFields geometry = GeoArrowFields.resolve(projectedSchema, geo);
        // Resolve once, before any byte is written: resolution rejects an unsupported leaf type and an unexportable
        // shape (a Parquet Variant nested under a list or map), and the resolved columns are reused for every batch.
        List<LogicalColumn> columns = LogicalColumns.of(projectedSchema, geometry);
        try {
            ByteBuffer schemaMessage = ArrowSchemaEncoder.encode(columns);
            IpcFraming.writeMessage(channel, schemaMessage, List.of());
            writeBatches(batches, columns, channel);
            IpcFraming.writeEndOfStream(channel);
        } catch (IOException e) {
            throw new UncheckedIOException("failed writing Arrow IPC stream", e);
        }
    }

    private static void writeBatches(
            Stream<ParquetRecordBatch> batches, List<LogicalColumn> columns, WritableByteChannel channel) {
        batches.forEach(batch -> {
            try (batch) {
                ArrowBatchEncoder.Encoded encoded = ArrowBatchEncoder.encode(batch, columns);
                IpcFraming.writeMessage(channel, encoded.metadata(), encoded.body());
            } catch (IOException e) {
                throw new UncheckedIOException("failed writing Arrow record batch", e);
            }
        });
    }
}
