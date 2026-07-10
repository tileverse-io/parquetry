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
package io.tileverse.parquetry.format;

/**
 * The bytes presented to parquetry are not a Parquet file at all - the file is too small or doesn't carry the
 * {@code PAR1} magic. Distinct from {@link MalformedFileException}, which means "this should have been Parquet but the
 * bytes are corrupt"; consumers can use this to route the file to a different format reader or skip it without logging
 * it as a Parquet corruption.
 */
@SuppressWarnings("serial")
public class NotAParquetFileException extends ParquetFormatException {

    public NotAParquetFileException(String message) {
        super(message);
    }

    public NotAParquetFileException(String message, Throwable cause) {
        super(message, cause);
    }

    public NotAParquetFileException(String message, long byteOffset, String field) {
        super(message, byteOffset, field);
    }

    public NotAParquetFileException(String message, long byteOffset, String field, Throwable cause) {
        super(message, byteOffset, field, cause);
    }

    @Override
    public NotAParquetFileException withContext(String prefix) {
        return new NotAParquetFileException(prefix, this);
    }

    @Override
    public NotAParquetFileException withContext(String prefix, long byteOffset, String field) {
        return new NotAParquetFileException(prefix, byteOffset, field, this);
    }
}
