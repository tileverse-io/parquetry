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
package io.tileverse.parquetry.format;

/**
 * Compression applied to a Parquet bloom filter's bitset. Mirror of the {@code BloomFilterCompression} Thrift union in
 * {@code parquet.thrift}.
 *
 * <p>The Thrift schema models this as a union of empty structs (currently only {@code Uncompressed}); since every
 * defined variant carries no payload, parquetry surfaces the union as a plain enum.
 */
public enum BloomFilterCompression {
    /** No compression; the bitset bytes follow the header verbatim. */
    UNCOMPRESSED
}
