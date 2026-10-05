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

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.RangeRequest;

import io.tileverse.parquetry.internal.read.IndexSectionRange;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.observe.FetchAccumulator;
import io.tileverse.parquetry.observe.FetchPurpose;

/**
 * The index sections of a phase are read as one batch into one buffer. Sections named by a corrupt footer may sum
 * beyond the file, or beyond the largest buffer: such a batch is not read, and each section is left to its own lookup.
 */
class FooterIndexSectionLoaderTest {

    private static final long GB = 1L << 30;

    @Test
    void sectionsWithinTheFileAreReadAsOneBatch() {
        RecordingSource source = new RecordingSource(1000L);
        FooterIndexSectionLoader loader = new FooterIndexSectionLoader(source, FetchAccumulator.NONE);

        loader.prefetch(List.of(columnIndex(100L, 50), columnIndex(150L, 50), offsetIndex(400L, 20)));

        assertThat(source.batchReads).isEqualTo(1);
    }

    @Test
    void sectionsSummingBeyondTheFileAreNotReadAsABatch() {
        RecordingSource source = new RecordingSource(1000L);
        FooterIndexSectionLoader loader = new FooterIndexSectionLoader(source, FetchAccumulator.NONE);

        loader.prefetch(List.of(columnIndex(0L, 600), columnIndex(100L, 600), offsetIndex(300L, 600)));

        assertThat(source.batchReads).isZero();
    }

    @Test
    void sectionsSummingBeyondTheLargestBufferAreNotReadAsABatch() {
        RecordingSource source = new RecordingSource(8 * GB);
        FooterIndexSectionLoader loader = new FooterIndexSectionLoader(source, FetchAccumulator.NONE);
        int oneGb = (int) GB;

        loader.prefetch(List.of(columnIndex(0L, oneGb), columnIndex(2 * GB, oneGb), offsetIndex(4 * GB, oneGb)));

        assertThat(source.batchReads).isZero();
    }

    private static IndexSectionRange columnIndex(long offset, int length) {
        return new IndexSectionRange(FetchPurpose.COLUMN_INDEX, offset, length);
    }

    private static IndexSectionRange offsetIndex(long offset, int length) {
        return new IndexSectionRange(FetchPurpose.OFFSET_INDEX, offset, length);
    }

    /** A source of zeros of a declared size, counting the batch reads asked of it. */
    private static final class RecordingSource implements ByteRangeSource {

        private final long size;
        private int batchReads;

        RecordingSource(long size) {
            this.size = size;
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public int read(long offset, MemorySegment dst) {
            return (int) dst.byteSize();
        }

        @Override
        public BatchReadResult readFully(List<RangeRequest> requests) {
            batchReads++;
            return ByteRangeSource.super.readFully(requests);
        }

        @Override
        public void close() {
            // nothing to release
        }
    }
}
