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
 * The file is structurally invalid Parquet - truncated mid-page, missing a required Thrift field, varints that don't
 * terminate, page cursors that fail to advance, and similar mid-stream corruption. Distinct from
 * {@link NotAParquetFileException} (the magic / size says it isn't even a Parquet file) and from
 * {@link UnknownCodeException} (a known field but an unknown variant id).
 */
@SuppressWarnings("serial")
public class MalformedFileException extends ParquetFormatException {

    public MalformedFileException(String message) {
        super(message);
    }

    public MalformedFileException(String message, Throwable cause) {
        super(message, cause);
    }

    public MalformedFileException(String message, long byteOffset, String field) {
        super(message, byteOffset, field);
    }

    public MalformedFileException(String message, long byteOffset, String field, Throwable cause) {
        super(message, byteOffset, field, cause);
    }

    @Override
    public MalformedFileException withContext(String prefix) {
        return new MalformedFileException(prefix, this);
    }

    @Override
    public MalformedFileException withContext(String prefix, long byteOffset, String field) {
        return new MalformedFileException(prefix, byteOffset, field, this);
    }
}
