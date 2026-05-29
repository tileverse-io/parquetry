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
package io.tileverse.parquetry.format;

/**
 * A Thrift enum wire code or union field id parquetry does not recognize - typically because the file is using a newer
 * Parquet specification than this version of parquetry models. Thrown by the {@code valueOf(int)} lookups on the format
 * enums.
 *
 * <p>This is the right subclass to catch for forward-compatibility tolerance ("we don't know what this code is, so skip
 * the column / file but don't fail the whole read"). Distinct from {@link UnsupportedFeatureException}, which means "we
 * recognize what this is, but we don't read it yet".
 */
@SuppressWarnings("serial")
public class UnknownCodeException extends ParquetFormatException {

    public UnknownCodeException(String message) {
        super(message);
    }

    public UnknownCodeException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnknownCodeException(String message, long byteOffset, String field) {
        super(message, byteOffset, field);
    }

    public UnknownCodeException(String message, long byteOffset, String field, Throwable cause) {
        super(message, byteOffset, field, cause);
    }

    @Override
    public UnknownCodeException withContext(String prefix) {
        return new UnknownCodeException(prefix, this);
    }

    @Override
    public UnknownCodeException withContext(String prefix, long byteOffset, String field) {
        return new UnknownCodeException(prefix, byteOffset, field, this);
    }
}
