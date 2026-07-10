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
package io.tileverse.parquetry.data;

import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.internal.filter.bloom.BloomFilterReader;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.internal.read.IndexSectionLoader;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.observe.FetchAccumulator;
import io.tileverse.parquetry.observe.FetchPurpose;

/**
 * Binds index-section reads to a {@link ByteRangeSource} and records each section's bytes into a
 * {@link FetchAccumulator}: a column-index read as {@link FetchPurpose#COLUMN_INDEX}, an offset-index read as
 * {@link FetchPurpose#OFFSET_INDEX}, and a bloom-filter read as {@link FetchPurpose#BLOOM_FILTER}. The recorded byte
 * count is the section's on-disk length. A bloom filter whose length the writer recorded counts the full chunk (header
 * plus bitset); one whose length is absent counts the bitset byte size discovered from the filter header, the only
 * figure cheaply available on that path. Passing {@link FetchAccumulator#NONE} reduces every record call to a no-op.
 */
final class FooterIndexSectionLoader implements IndexSectionLoader {

    private final ByteRangeSource source;
    private final FetchAccumulator accumulator;

    FooterIndexSectionLoader(ByteRangeSource source, FetchAccumulator accumulator) {
        this.source = source;
        this.accumulator = accumulator;
    }

    @Override
    public OffsetIndex readOffsetIndex(long offset, int length) {
        OffsetIndex offsetIndex = ParquetFormat.readOffsetIndex(source, offset, length);
        accumulator.add(FetchPurpose.OFFSET_INDEX, length);
        return offsetIndex;
    }

    @Override
    public ColumnIndex readColumnIndex(long offset, int length) {
        ColumnIndex columnIndex = ParquetFormat.readColumnIndex(source, offset, length);
        accumulator.add(FetchPurpose.COLUMN_INDEX, length);
        return columnIndex;
    }

    @Override
    public SplitBlockBloomFilter readBloom(long offset, int length) {
        if (length > 0) {
            SplitBlockBloomFilter filter = BloomFilterReader.read(source, offset, length);
            accumulator.add(FetchPurpose.BLOOM_FILTER, length);
            return filter;
        }
        SplitBlockBloomFilter filter = BloomFilterReader.readWithoutLength(source, offset);
        accumulator.add(FetchPurpose.BLOOM_FILTER, filter.byteSize());
        return filter;
    }
}
