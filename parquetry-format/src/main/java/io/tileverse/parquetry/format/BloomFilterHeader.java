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
 * Header that precedes a column chunk's bloom-filter bitset; mirror of {@code BloomFilterHeader} in
 * {@code parquet.thrift}.
 *
 * <p>Located at {@link ColumnMetaData#bloomFilterOffset()} within the file. The header is Thrift-compact-encoded; the
 * bitset follows immediately after it and spans exactly {@link #numBytes()} bytes.
 *
 * @param numBytes size of the bitset in bytes (always a multiple of 32 in a valid Parquet file)
 * @param algorithm bit-setting algorithm; currently only {@link BloomFilterAlgorithm#SPLIT_BLOCK} is defined
 * @param hash hash function applied to plain-encoded values; currently only {@link BloomFilterHashStrategy#XXHASH}
 * @param compression compression applied to the bitset; currently only {@link BloomFilterCompression#UNCOMPRESSED}
 */
public record BloomFilterHeader(
        int numBytes,
        BloomFilterAlgorithm algorithm,
        BloomFilterHashStrategy hash,
        BloomFilterCompression compression) {}
