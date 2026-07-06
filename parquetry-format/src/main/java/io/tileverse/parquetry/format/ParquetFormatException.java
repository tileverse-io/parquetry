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
package io.tileverse.parquetry.format;

import java.util.OptionalLong;

/**
 * Thrown when on-disk bytes do not conform to the Parquet/Thrift specification. When known, the exception surfaces the
 * byte offset and the name of the field being parsed so operators can locate the corruption.
 *
 * <p>Unchecked because file-format errors typically surface mid-iteration through the {@code Stream<ParquetRecord>}
 * returned by {@code ParquetSource.read()}, where checked exceptions can't propagate without lossy wrapping.
 *
 * <h2>Subclass hierarchy</h2>
 *
 * <p>The leaf throw sites use one of four concrete subtypes so library consumers can write precise catches:
 *
 * <ul>
 *   <li>{@link NotAParquetFileException} - bad magic / file too small; the bytes aren't a Parquet file at all.
 *   <li>{@link UnsupportedFeatureException} - known Parquet feature parquetry doesn't read yet (e.g. encrypted files
 *       pending the parquetry-encryption module).
 *   <li>{@link UnknownCodeException} - a Thrift enum wire code or union field id parquetry doesn't recognize; the file
 *       may be using a newer Parquet spec than this version models. Suitable for forward-compat tolerant catches.
 *   <li>{@link MalformedFileException} - structurally broken Parquet (truncated, missing required Thrift field, bad
 *       compact-protocol bytes).
 * </ul>
 *
 * <p>Wrap sites that re-throw with added context use {@link #withContext(String)} (or its 3-arg sibling) to preserve
 * the concrete subclass, so a {@code catch (MalformedFileException)} at any level still fires.
 */
@SuppressWarnings("serial")
public class ParquetFormatException extends RuntimeException {

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

    /**
     * Returns a new exception of the same concrete subclass, with {@code prefix} as the wrapper message and
     * {@code this} as the cause. Lets wrap-and-rethrow sites add context without flattening the leaf type to plain
     * {@code ParquetFormatException} - so {@code catch (MalformedFileException e)} continues to fire at outer
     * boundaries.
     *
     * <p>Subclasses override to return their own concrete type.
     */
    public ParquetFormatException withContext(String prefix) {
        return new ParquetFormatException(prefix, this);
    }

    /**
     * Same as {@link #withContext(String)} but also attaches a byte offset and field name to the wrapper. Used by
     * page-level wrap sites that know which structural field they were decoding when the inner failure surfaced.
     */
    public ParquetFormatException withContext(String prefix, long byteOffset, String field) {
        return new ParquetFormatException(prefix, byteOffset, field, this);
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
