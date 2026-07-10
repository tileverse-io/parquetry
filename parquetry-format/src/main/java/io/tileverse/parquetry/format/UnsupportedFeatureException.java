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
 * The file uses a Parquet feature parquetry recognizes but does not yet implement at this layer - typical examples are
 * encrypted files (which require the parquetry-encryption module to read) and bloom filters that declare an algorithm,
 * hash strategy, or compression parquetry doesn't yet decode. Distinct from {@link UnknownCodeException}, which means
 * "the case id is not defined in any spec parquetry knows about".
 */
@SuppressWarnings("serial")
public class UnsupportedFeatureException extends ParquetFormatException {

    public UnsupportedFeatureException(String message) {
        super(message);
    }

    public UnsupportedFeatureException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnsupportedFeatureException(String message, long byteOffset, String field) {
        super(message, byteOffset, field);
    }

    public UnsupportedFeatureException(String message, long byteOffset, String field, Throwable cause) {
        super(message, byteOffset, field, cause);
    }

    @Override
    public UnsupportedFeatureException withContext(String prefix) {
        return new UnsupportedFeatureException(prefix, this);
    }

    @Override
    public UnsupportedFeatureException withContext(String prefix, long byteOffset, String field) {
        return new UnsupportedFeatureException(prefix, byteOffset, field, this);
    }
}
