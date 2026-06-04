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
package io.tileverse.parquetry.internal.write;

/**
 * Placement metadata returned by {@link BloomFilterBuilder#writeTo}. Carries the byte offset where the bloom filter
 * (header + bitset) was written and the total length it consumed, in the shape the column-chunk writer needs to record
 * in {@code ColumnMetaData.bloom_filter_offset} and {@code bloom_filter_length}.
 *
 * @param byteOffset byte offset in the surrounding file where the bloom filter (header + bitset) starts
 * @param byteLength total bytes written: Thrift-compact header followed by the raw SBBF bitset
 */
public record BloomFilterPlacement(long byteOffset, int byteLength) {}
