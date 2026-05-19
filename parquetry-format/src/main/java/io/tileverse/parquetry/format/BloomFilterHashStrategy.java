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
 * The hash function used by a Parquet bloom filter. Mirror of the {@code BloomFilterHash} Thrift union in
 * {@code parquet.thrift}.
 *
 * <p>The Thrift schema models this as a union of empty structs (currently only {@code XxHash}); since every defined
 * variant carries no payload, parquetry surfaces the union as a plain enum.
 */
public enum BloomFilterHashStrategy {
    /** xxHash64 with seed 0, applied to the plain-encoded bytes of the column value. */
    XXHASH
}
