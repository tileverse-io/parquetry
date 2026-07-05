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
package io.tileverse.parquetry.dataset;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * Consumer-side flattening of a decoded batch to rows. A multi-file fan-out transports whole batches across its
 * producer-to-consumer hand-off, where each batch owns its data; the rows a batch holds are flyweight views over that
 * batch's memory, and this flatten runs on the consuming thread precisely to keep those views from crossing a thread
 * boundary.
 */
final class BatchRows {

    private BatchRows() {}

    /**
     * Flattens one decoded batch to rows via {@code materializer}, closing the batch when the returned row stream
     * closes. Runs on the consuming thread: the row views over batch memory never cross a thread boundary.
     */
    static <T> Stream<T> flatten(ParquetRecordBatch batch, Materializer<T> materializer) {
        ParquetSchema producedSchema = batch.projectedSchema();
        return IntStream.range(0, batch.rowCount())
                .mapToObj(row -> materializer.materialize(producedSchema, batch.materialize(row)))
                .onClose(batch::close);
    }
}
