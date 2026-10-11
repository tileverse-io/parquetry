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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.RangeRequest;

import io.tileverse.parquetry.format.BloomFilterHeader;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.internal.read.IndexSectionRange;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.observe.FetchAccumulator;
import io.tileverse.parquetry.observe.FetchPurpose;

/**
 * The index sections of a phase are read as one batch into one buffer. Sections named by a corrupt footer may sum
 * beyond the file, or beyond the largest buffer: such a batch is not read, and each section is left to its own lookup.
 * A section lies outside the column chunks of its file, and one declared longer than the bytes left there is refused
 * before a buffer of that length is allocated.
 */
class FooterIndexSectionLoaderTest {

    private static final long GB = 1L << 30;

    @Test
    void sectionsWithinTheFileAreReadAsOneBatch() {
        RecordingSource source = new RecordingSource(1000L);
        FooterIndexSectionLoader loader = new FooterIndexSectionLoader(source, FetchAccumulator.NONE, 0L);

        loader.prefetch(List.of(columnIndex(100L, 50), columnIndex(150L, 50), offsetIndex(400L, 20)));

        assertThat(source.batchReads).isEqualTo(1);
    }

    @Test
    void sectionsSummingBeyondTheFileAreNotReadAsABatch() {
        RecordingSource source = new RecordingSource(1000L);
        FooterIndexSectionLoader loader = new FooterIndexSectionLoader(source, FetchAccumulator.NONE, 0L);

        loader.prefetch(List.of(columnIndex(0L, 600), columnIndex(100L, 600), offsetIndex(300L, 600)));

        assertThat(source.batchReads).isZero();
    }

    @Test
    void sectionsSummingBeyondTheLargestBufferAreNotReadAsABatch() {
        RecordingSource source = new RecordingSource(8 * GB);
        FooterIndexSectionLoader loader = new FooterIndexSectionLoader(source, FetchAccumulator.NONE, 0L);
        int oneGb = (int) GB;

        loader.prefetch(List.of(columnIndex(0L, oneGb), columnIndex(2 * GB, oneGb), offsetIndex(4 * GB, oneGb)));

        assertThat(source.batchReads).isZero();
    }

    // A page index longer than the bytes of the file outside its column chunks is refused before a read.
    @Test
    void refusesOversizedPageIndex() {
        RecordingSource source = new RecordingSource(1000L);
        long columnChunkBytes = 900L;
        FooterIndexSectionLoader loader = new FooterIndexSectionLoader(source, FetchAccumulator.NONE, columnChunkBytes);

        assertThatThrownBy(() -> loader.readColumnIndex(100L, 500))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("500 bytes")
                .hasMessageContaining("100 bytes outside its column chunks");
        assertThatThrownBy(() -> loader.readOffsetIndex(100L, 101))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("100 bytes outside its column chunks");
        assertThat(source.reads).isZero();
    }

    @Test
    void refusesOversizedBloomFilter() {
        RecordingSource source = new RecordingSource(1000L);
        long columnChunkBytes = 900L;
        FooterIndexSectionLoader loader = new FooterIndexSectionLoader(source, FetchAccumulator.NONE, columnChunkBytes);

        assertThatThrownBy(() -> loader.readBloom(100L, 500))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("100 bytes outside its column chunks");
        assertThat(source.reads).isZero();
    }

    // Sections summing beyond the bytes outside the column chunks are not fetched as one batch.
    @Test
    void skipsOversizedBatch() {
        RecordingSource source = new RecordingSource(1000L);
        long columnChunkBytes = 900L;
        FooterIndexSectionLoader loader = new FooterIndexSectionLoader(source, FetchAccumulator.NONE, columnChunkBytes);

        loader.prefetch(List.of(columnIndex(100L, 50), columnIndex(150L, 50), offsetIndex(400L, 20)));

        assertThat(source.batchReads).isZero();
    }

    @Test
    void readsBatchOfFittingSections() {
        RecordingSource source = new RecordingSource(1000L);
        long columnChunkBytes = 880L;
        FooterIndexSectionLoader loader = new FooterIndexSectionLoader(source, FetchAccumulator.NONE, columnChunkBytes);

        loader.prefetch(List.of(columnIndex(100L, 50), columnIndex(150L, 50), offsetIndex(400L, 20)));

        assertThat(source.batchReads).isEqualTo(1);
    }

    // Chunk sizes summing to the file size or beyond leave no byte for a section.
    @ParameterizedTest(name = "chunks of {0} bytes")
    @ValueSource(longs = {1000L, 5000L, Long.MAX_VALUE})
    void chunksFillingTheFile(long columnChunkBytes) {
        RecordingSource source = new RecordingSource(1000L);
        FooterIndexSectionLoader loader = new FooterIndexSectionLoader(source, FetchAccumulator.NONE, columnChunkBytes);

        assertThatThrownBy(() -> loader.readColumnIndex(100L, 1))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("0 bytes outside its column chunks");
        loader.prefetch(List.of(columnIndex(100L, 10), offsetIndex(200L, 10)));
        assertThat(source.batchReads).isZero();
        assertThat(source.reads).isZero();
    }

    // A negative sum of chunk sizes proves nothing: the file size stays as the limit.
    @ParameterizedTest(name = "chunks of {0} bytes")
    @ValueSource(longs = {-1L, Long.MIN_VALUE})
    void negativeChunkBytes(long columnChunkBytes) {
        RecordingSource source = new RecordingSource(1000L);
        FooterIndexSectionLoader loader = new FooterIndexSectionLoader(source, FetchAccumulator.NONE, columnChunkBytes);

        loader.prefetch(List.of(columnIndex(0L, 500), offsetIndex(500L, 500)));
        assertThat(source.batchReads).isEqualTo(1);

        assertThatThrownBy(() -> loader.readColumnIndex(1L, 1000))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("reaches outside the file");
    }

    // Without a declared length, the bitset size in the header of a bloom filter is held to the same limit.
    @Test
    void refusesOversizedBloomBitset(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("bloom.bin");
        Files.write(file, bloomHeaderDeclaringABitsetOf(320, 700));
        int lengthLeftToTheHeader = 0;

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            long columnChunkBytes = source.size() - 100L;
            FooterIndexSectionLoader loader =
                    new FooterIndexSectionLoader(source, FetchAccumulator.NONE, columnChunkBytes);

            assertThatThrownBy(() -> loader.readBloom(0L, lengthLeftToTheHeader))
                    .isInstanceOf(MalformedFileException.class)
                    .hasMessageContaining("bitset of 320 bytes")
                    .hasMessageContaining("limit of 100 bytes");
        }
    }

    /** The header of a bloom filter declaring {@code bitsetBytes}, then {@code trailingBytes} zeros. */
    private static byte[] bloomHeaderDeclaringABitsetOf(int bitsetBytes, int trailingBytes) {
        BloomFilterHeader header = new BloomFilterHeader(
                bitsetBytes,
                BloomFilterHeader.Algorithm.SPLIT_BLOCK,
                BloomFilterHeader.HashStrategy.XXHASH,
                BloomFilterHeader.Compression.UNCOMPRESSED);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ParquetFormat.writeBloomFilterHeader(bytes, header);
        bytes.writeBytes(new byte[trailingBytes]);
        return bytes.toByteArray();
    }

    private static IndexSectionRange columnIndex(long offset, int length) {
        return new IndexSectionRange(FetchPurpose.COLUMN_INDEX, offset, length);
    }

    private static IndexSectionRange offsetIndex(long offset, int length) {
        return new IndexSectionRange(FetchPurpose.OFFSET_INDEX, offset, length);
    }

    /** A source of zeros of a declared size, counting the reads and the batch reads asked of it. */
    private static final class RecordingSource implements ByteRangeSource {

        private final long size;
        private int batchReads;
        private int reads;

        RecordingSource(long size) {
            this.size = size;
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public int read(long offset, MemorySegment dst) {
            reads++;
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
