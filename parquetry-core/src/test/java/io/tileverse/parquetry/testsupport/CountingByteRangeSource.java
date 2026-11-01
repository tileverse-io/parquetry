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
package io.tileverse.parquetry.testsupport;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.RangeRequest;

import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * A {@link ByteRangeSource} that delegates to a backing source and counts the bytes and the requests of the reads made
 * through it. A named one answers to the name of its delegate, and the caches of the engine key on the file as they do
 * for the delegate. An unnamed one has no name, like a source without a stable identity.
 *
 * <p>A prefetching read fetches on virtual threads, beside the consumer thread. The counters are atomic, and a test
 * reads them once the read is over.
 */
public final class CountingByteRangeSource implements ByteRangeSource {

    private final ByteRangeSource delegate;
    private final Optional<String> name;
    private final AtomicLong bytes = new AtomicLong();
    private final AtomicLong requests = new AtomicLong();

    private CountingByteRangeSource(ByteRangeSource delegate, Optional<String> name) {
        this.delegate = delegate;
        this.name = name;
    }

    /** Counts the reads of {@code delegate}, under the name of {@code delegate}. */
    public static CountingByteRangeSource named(ByteRangeSource delegate) {
        return new CountingByteRangeSource(delegate, delegate.sourceIdentifier());
    }

    /** Counts the reads of {@code delegate}, under no name. */
    public static CountingByteRangeSource unnamed(ByteRangeSource delegate) {
        return new CountingByteRangeSource(delegate, Optional.empty());
    }

    @Override
    public long size() {
        return delegate.size();
    }

    @Override
    public Optional<String> sourceIdentifier() {
        return name;
    }

    @Override
    public int read(long offset, MemorySegment dst) {
        int read = delegate.read(offset, dst);
        if (read > 0) {
            count(read);
        }
        return read;
    }

    /** Counts the read as one request. The delegate may read in several calls, and they count as one. */
    @Override
    public void readFully(long offset, MemorySegment dst) {
        delegate.readFully(offset, dst);
        count(dst.byteSize());
    }

    /**
     * Counts each range of the batch as one request, and forwards the batch to the delegate in one call. The interface
     * default would read the ranges one at a time through this source.
     */
    @Override
    public BatchReadResult readFully(List<RangeRequest> batch) {
        BatchReadResult result = delegate.readFully(batch);
        for (RangeRequest request : batch) {
            count(request.range().length());
        }
        return result;
    }

    @Override
    public void close() {
        delegate.close();
    }

    /** The bytes read since the last {@link #reset}. */
    public long bytes() {
        return bytes.get();
    }

    /** The requests made since the last {@link #reset}, one per single read and one per range of a batch. */
    public long requests() {
        return requests.get();
    }

    /** Starts both counts again from zero, before the read to measure. */
    public void reset() {
        bytes.set(0);
        requests.set(0);
    }

    private void count(long length) {
        bytes.addAndGet(length);
        requests.incrementAndGet();
    }
}
