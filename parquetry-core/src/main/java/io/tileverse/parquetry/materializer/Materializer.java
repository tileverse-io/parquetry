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
package io.tileverse.parquetry.materializer;

import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.Schema;

/**
 * Strategy that turns an assembled {@link RowAccessor} into the caller's preferred row type.
 *
 * <p>Implementations must be stateless and safe to invoke per row from a single reader thread. The reader supplies the
 * projected schema once and feeds successive row accessors as it iterates the dataset; the materializer owns the cost
 * of adapting them to {@code T}.
 *
 * <p>The default implementation, available via {@link #defaultRecord()}, returns the canonical lazy
 * {@link ParquetRecord} view. Custom user types plug in by implementing this interface directly.
 */
public interface Materializer<T> {

    /** Builds one value of {@code T} from a row already assembled against {@code projectedSchema}. */
    T materialize(Schema projectedSchema, RowAccessor row);

    /** The canonical built-in: produces a lazy {@link ParquetRecord} backed directly by the row accessor. */
    static Materializer<ParquetRecord> defaultRecord() {
        return DefaultMaterializer.INSTANCE;
    }
}
