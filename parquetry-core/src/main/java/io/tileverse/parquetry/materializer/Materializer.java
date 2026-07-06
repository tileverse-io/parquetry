/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.materializer;

import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * Strategy that turns an assembled {@link ParquetRecord} into the caller's preferred row type.
 *
 * <p>Implementations must be stateless and safe to invoke per row from a single reader thread. The reader supplies the
 * projected schema once and feeds successive records as it iterates the dataset; the materializer owns the cost of
 * adapting them to {@code T}.
 *
 * <p>The record is a live, batch-lifetime view: a materializer reads the values it needs during the call and either
 * produces an owned {@code T} or retains the record only after calling {@link ParquetRecord#detach()}.
 *
 * <p>The default implementation, available via {@link #defaultRecord()}, returns the record itself. Custom user types
 * plug in by implementing this interface directly.
 */
public interface Materializer<T> {

    /** Builds one value of {@code T} from a {@code row} already assembled against {@code projectedSchema}. */
    T materialize(ParquetSchema projectedSchema, ParquetRecord row);

    /** The canonical built-in: produces a lazy {@link ParquetRecord} backed directly by the row accessor. */
    static Materializer<ParquetRecord> defaultRecord() {
        return DefaultMaterializer.INSTANCE;
    }
}
