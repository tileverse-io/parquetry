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

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.RangeRequest;

import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.internal.filter.bloom.BloomFilterReader;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.internal.read.IndexSectionLoader;
import io.tileverse.parquetry.internal.read.IndexSectionRange;
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
 *
 * <p>{@link #prefetch} reads a whole phase's sections in one call and holds their bytes until each is decoded. A batch
 * spanning both kinds of index section keeps its byte tallies split by kind and still counts one call, reporting what
 * the call cost under the first of those kinds.
 *
 * <p>Confined to the read's consumer thread, like the {@code RowGroupChunks} views that drive it.
 */
final class FooterIndexSectionLoader implements IndexSectionLoader {

    private final ByteRangeSource source;
    private final FetchAccumulator accumulator;

    /** The bytes of a prefetched batch, keyed by file offset and removed by the read that decodes them. */
    private final Map<Long, MemorySegment> held = new HashMap<>();

    FooterIndexSectionLoader(ByteRangeSource source, FetchAccumulator accumulator) {
        this.source = source;
        this.accumulator = accumulator;
    }

    @Override
    public OffsetIndex readOffsetIndex(long offset, int length) {
        return ParquetFormat.readOffsetIndex(sectionBytes(FetchPurpose.OFFSET_INDEX, offset, length));
    }

    @Override
    public ColumnIndex readColumnIndex(long offset, int length) {
        return ParquetFormat.readColumnIndex(sectionBytes(FetchPurpose.COLUMN_INDEX, offset, length));
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

    @Override
    public void prefetch(List<IndexSectionRange> sections) {
        dropBytesHeldFromAnEarlierBatch();
        List<IndexSectionRange> batch = inFileOrder(sections);
        if (batch.size() < 2) {
            return;
        }
        byte[] bytes = new byte[totalLength(batch)];
        BatchReadResult cost = source.readFully(requestsOver(batch, bytes));
        hold(batch, bytes);
        recordBatch(batch, cost);
    }

    /**
     * Forgets the bytes of an earlier batch. A phase consumes its own batch before the next phase asks for one;
     * anything left over is a section never asked for by a short-circuit, and holding it past its phase only retains
     * bytes.
     */
    private void dropBytesHeldFromAnEarlierBatch() {
        held.clear();
    }

    private static List<IndexSectionRange> inFileOrder(List<IndexSectionRange> sections) {
        List<IndexSectionRange> ordered = new ArrayList<>(sections);
        ordered.sort((left, right) -> Long.compare(left.fileOffset(), right.fileOffset()));
        return ordered;
    }

    private static int totalLength(List<IndexSectionRange> batch) {
        int total = 0;
        for (IndexSectionRange section : batch) {
            total += section.length();
        }
        return total;
    }

    /**
     * One request per section, each targeting the window of {@code bytes} where that section lands. The windows lie
     * back to back in file order over one array, which lets a byte source with no batch read beneath it join two
     * sections written next to each other into a single read.
     */
    private static List<RangeRequest> requestsOver(List<IndexSectionRange> batch, byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        List<RangeRequest> requests = new ArrayList<>(batch.size());
        int position = 0;
        for (IndexSectionRange section : batch) {
            requests.add(RangeRequest.of(section.fileOffset(), section.length(), window(buffer, position, section)));
            position += section.length();
        }
        return requests;
    }

    /**
     * A window on {@code buffer} at {@code position}, the section's length wide. A duplicate rather than a slice: a
     * joined read of two touching sections widens the first window's limit, which needs the whole array's capacity
     * behind it.
     */
    private static ByteBuffer window(ByteBuffer buffer, int position, IndexSectionRange section) {
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.limit(position + section.length());
        duplicate.position(position);
        return duplicate;
    }

    private void hold(List<IndexSectionRange> batch, byte[] bytes) {
        MemorySegment whole = MemorySegment.ofArray(bytes);
        long position = 0;
        for (IndexSectionRange section : batch) {
            held.put(section.fileOffset(), whole.asSlice(position, section.length()));
            position += section.length();
        }
    }

    /**
     * Records the batch as one call, with its bytes and its ranges still split by kind of section: the first kind
     * reports the call together with its cost, and every further kind joins that call. Counting the call once per kind,
     * or repeating the cost, would report several calls where the byte source saw one.
     */
    private void recordBatch(List<IndexSectionRange> batch, BatchReadResult cost) {
        boolean callReported = false;
        for (Map.Entry<FetchPurpose, Tally> kind : talliesByPurpose(batch).entrySet()) {
            Tally tally = kind.getValue();
            if (callReported) {
                accumulator.addRangesOfSameCall(kind.getKey(), tally.bytes(), tally.requests());
                continue;
            }
            accumulator.add(
                    kind.getKey(),
                    tally.bytes(),
                    tally.requests(),
                    cost.fetches(),
                    cost.bytesTransferred(),
                    cost.bytesFromCache());
            callReported = true;
        }
    }

    private static Map<FetchPurpose, Tally> talliesByPurpose(List<IndexSectionRange> batch) {
        Map<FetchPurpose, Tally> tallies = new EnumMap<>(FetchPurpose.class);
        for (IndexSectionRange section : batch) {
            Tally tally = tallies.computeIfAbsent(section.purpose(), _ -> new Tally());
            tally.add(section.length());
        }
        return tallies;
    }

    /**
     * The section's bytes: the ones held for this offset by a prefetch when they span the length asked for, else a read
     * of its own, recorded when it reads. A malformed footer that names one offset under two lengths leaves only the
     * last of them held, and decoding those bytes as the other section would answer with plausible garbage.
     */
    private MemorySegment sectionBytes(FetchPurpose purpose, long offset, int length) {
        MemorySegment prefetched = held.remove(offset);
        if (prefetched != null && prefetched.byteSize() == length) {
            return prefetched;
        }
        MemorySegment bytes = MemorySegment.ofArray(new byte[length]);
        source.readFully(offset, bytes);
        accumulator.add(purpose, length);
        return bytes;
    }

    /** Running bytes and range count for one {@link FetchPurpose} within a batch. */
    private static final class Tally {

        private long bytes;
        private int requests;

        void add(int length) {
            bytes += length;
            requests++;
        }

        long bytes() {
            return bytes;
        }

        int requests() {
            return requests;
        }
    }
}
