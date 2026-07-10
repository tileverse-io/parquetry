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
package io.tileverse.parquetry.internal.read;

import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;

/**
 * Reads a column chunk's index sections from a backing store. {@link RowGroupChunks} depends on this minimal interface
 * rather than on a {@code ByteRangeSource} directly, which keeps it pure and lets tests drive it with a counting fake.
 *
 * <p>The reader binds an implementation to its data source; the loader is called at most once per column per section,
 * because {@link RowGroupChunks} memoizes the result.
 */
public interface IndexSectionLoader {

    /** Reads the {@link OffsetIndex} at {@code offset} spanning {@code length} bytes. */
    OffsetIndex readOffsetIndex(long offset, int length);

    /** Reads the {@link ColumnIndex} at {@code offset} spanning {@code length} bytes. */
    ColumnIndex readColumnIndex(long offset, int length);

    /**
     * Reads a column's split-block bloom filter at {@code offset}. A {@code length} of zero or less means the writer
     * did not record the bloom-filter length; the implementation then reads the header first to learn the bitset size.
     */
    SplitBlockBloomFilter readBloom(long offset, int length);
}
