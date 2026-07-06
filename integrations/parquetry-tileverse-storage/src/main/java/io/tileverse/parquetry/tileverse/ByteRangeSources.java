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
package io.tileverse.parquetry.tileverse;

import java.util.Objects;

import io.tileverse.storage.RangeReader;

import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * Factories that bridge tileverse-storage readers into parquetry's read SPI. The single-call entry point for tileverse
 * users: {@code ParquetSource.open(ByteRangeSources.from(rangeReader))}.
 */
public final class ByteRangeSources {

    private ByteRangeSources() {}

    /**
     * Wraps {@code reader} as a {@link ByteRangeSource}. The returned source BORROWS the reader; the caller still
     * closes the {@code RangeReader} after the last read (the source's own {@code close()} is a no-op).
     *
     * @throws IllegalStateException if the reader cannot report its size
     */
    public static ByteRangeSource from(RangeReader reader) {
        return new RangeReaderByteRangeSource(Objects.requireNonNull(reader, "reader"));
    }
}
