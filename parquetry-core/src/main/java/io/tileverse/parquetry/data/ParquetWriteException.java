/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.data;

/**
 * Thrown by the parquetry write engine when an operation fails for reasons internal to the writer. Plain I/O errors
 * from the underlying {@code OutputStream} propagate as {@link java.io.IOException} (or its wrapped
 * {@link java.io.UncheckedIOException} form when crossing stream boundaries); this exception is for our-side failures -
 * encoder rejection, schema mismatch caught at write time, structured-task-scope timeout, temp-file lifecycle problems.
 *
 * <p>Unchecked by design, matching the read side's domain exceptions ({@code ParquetFormatException},
 * {@code ParquetSchemaException}). Failures from a {@code ParquetFileWriter} typically emerge mid-iteration through a
 * caller's record stream; checked exceptions cannot flow through {@code Stream} APIs without lossy wrapping.
 */
public class ParquetWriteException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ParquetWriteException(String message) {
        super(message);
    }

    public ParquetWriteException(String message, Throwable cause) {
        super(message, cause);
    }
}
