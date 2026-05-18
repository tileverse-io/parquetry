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

import java.util.OptionalLong;

/**
 * Thrown when on-disk bytes do not conform to the Parquet/Thrift specification. When known, the exception surfaces the
 * byte offset and the name of the field being parsed so operators can locate the corruption.
 *
 * <p>Unchecked because file-format errors typically surface mid-iteration through the {@code Stream<ParquetRecord>}
 * returned by {@code Dataset.read()}, where checked exceptions can't propagate without lossy wrapping.
 */
public class ParquetFormatException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private static final long NO_OFFSET = -1L;

    private final long byteOffset;
    private final String field;

    public ParquetFormatException(String message) {
        super(message);
        this.byteOffset = NO_OFFSET;
        this.field = null;
    }

    public ParquetFormatException(String message, Throwable cause) {
        super(message, cause);
        this.byteOffset = NO_OFFSET;
        this.field = null;
    }

    public ParquetFormatException(String message, long byteOffset, String field) {
        this(message, byteOffset, field, null);
    }

    public ParquetFormatException(String message, long byteOffset, String field, Throwable cause) {
        super(formatMessage(message, byteOffset, field), cause);
        this.byteOffset = byteOffset;
        this.field = field;
    }

    /** The byte offset within the source where parsing failed, when known. */
    public OptionalLong byteOffset() {
        return byteOffset == NO_OFFSET ? OptionalLong.empty() : OptionalLong.of(byteOffset);
    }

    /** The name of the field being parsed when the error occurred, or {@code null} if not known. */
    public String field() {
        return field;
    }

    private static String formatMessage(String message, long offset, String field) {
        if (offset == NO_OFFSET && field == null) {
            return message;
        }
        StringBuilder sb = new StringBuilder(message);
        sb.append(" (");
        if (field != null) {
            sb.append("field=").append(field);
            if (offset != NO_OFFSET) {
                sb.append(", ");
            }
        }
        if (offset != NO_OFFSET) {
            sb.append("offset=").append(offset);
        }
        sb.append(')');
        return sb.toString();
    }
}
