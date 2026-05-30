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
package io.tileverse.parquetry.arrow;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.stream.Stream;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.format.UnsupportedFeatureException;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ParquetSchemaException;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Writes an Arrow IPC stream (streaming format) for a sequence of {@link ParquetRecordBatch}: a Schema message, one
 * RecordBatch message per batch, then end-of-stream. The projected schema is validated up front; an unsupported column
 * fails before any byte is written. The caller's {@link OutputStream} is flushed but left open.
 */
public final class ArrowIpcWriter {

    private ArrowIpcWriter() {}

    public static void write(
            ParquetSchema projectedSchema,
            Optional<GeoParquetMetadata> geo,
            Stream<ParquetRecordBatch> batches,
            OutputStream out) {
        GeoArrowFields geometry = GeoArrowFields.resolve(projectedSchema, geo);
        validate(projectedSchema);
        try {
            ByteBuffer schemaMessage = ArrowSchemaEncoder.encode(projectedSchema, geometry);
            IpcFraming.writeMessage(out, schemaMessage, new byte[0]);
            writeBatches(batches, out);
            IpcFraming.writeEndOfStream(out);
            out.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("failed writing Arrow IPC stream", e);
        }
    }

    private static void writeBatches(Stream<ParquetRecordBatch> batches, OutputStream out) {
        batches.forEach(batch -> {
            try (batch) {
                ArrowBatchEncoder.Encoded encoded = ArrowBatchEncoder.encode(batch);
                IpcFraming.writeMessage(out, encoded.metadata(), encoded.body());
            } catch (IOException e) {
                throw new UncheckedIOException("failed writing Arrow record batch", e);
            }
        });
    }

    private static void validate(ParquetSchema schema) {
        for (ColumnPath path : schema.leafColumns()) {
            SchemaNode node = schema.find(path)
                    .orElseThrow(() -> new ParquetSchemaException("no schema node for column " + path.dot()));
            if (node instanceof SchemaNode.Primitive primitive) {
                ArrowFieldType.of(primitive);
            } else {
                throw new UnsupportedFeatureException("Arrow output does not support nested column " + path.dot());
            }
        }
    }
}
