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
package io.tileverse.parquetry;

/**
 * Thrown when a caller's {@code Predicate} or {@code Projection} references a column that does not exist in the file
 * schema, or where the value type is incompatible with the column's primitive kind (e.g. {@code Eq(intCol,
 * StringVal)}).
 *
 * <p>Raised eagerly at {@code Dataset.read()} so callers get a clear error before any I/O is performed.
 */
public class ParquetSchemaException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ParquetSchemaException(String message) {
        super(message);
    }

    public ParquetSchemaException(String message, Throwable cause) {
        super(message, cause);
    }
}
