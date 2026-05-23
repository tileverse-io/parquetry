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
package io.tileverse.parquetry.schema;

/**
 * Thrown when a caller references a column that does not exist in the file schema, or where the value type is
 * incompatible with the column's primitive kind (e.g. {@code Eq(intCol, StringVal)}, or
 * {@code row.getLong(stringCol)}).
 *
 * <p>Raised at the moment the caller-supplied column identity is checked against the schema: eagerly at
 * {@code ParquetDataset.read()} setup time for {@code Predicate} and {@code Projection}, lazily per-cell for row and
 * batch accessors that the caller drives after a record is materialized. Either way the condition describes a
 * programming error in the caller (a malformed predicate, projection, or accessor built against the wrong schema), not
 * a recoverable I/O state.
 *
 * <p>Unchecked by design. Callers that want to react to it catch the type explicitly; all other callers let it
 * propagate. See the package documentation for the full rationale on unchecked exceptions across the parquetry API.
 */
@SuppressWarnings("serial")
public class ParquetSchemaException extends RuntimeException {

    public ParquetSchemaException(String message) {
        super(message);
    }

    public ParquetSchemaException(String message, Throwable cause) {
        super(message, cause);
    }
}
