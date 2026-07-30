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
package io.tileverse.parquetry.io;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A {@link ByteRangeSource} decorator that records every {@link #read(long, MemorySegment)} call as an {@code (offset,
 * length)} range, where {@code length} is the byte count the delegate actually returned. Byte and request assertions in
 * read tests derive from the recorded list. Recording is thread-safe: prefetch reads run concurrently on virtual
 * threads.
 */
public final class RecordingByteRangeSource implements ByteRangeSource {

    /** One recorded read: the requested offset and the byte count the delegate returned (0 for an EOF probe). */
    public record Range(long offset, long length) {}

    private final ByteRangeSource delegate;
    private final List<Range> ranges = Collections.synchronizedList(new ArrayList<>());

    public RecordingByteRangeSource(ByteRangeSource delegate) {
        this.delegate = delegate;
    }

    @Override
    public long size() {
        return delegate.size();
    }

    @Override
    public int read(long offset, MemorySegment dst) {
        int read = delegate.read(offset, dst);
        ranges.add(new Range(offset, Math.max(read, 0)));
        return read;
    }

    @Override
    public void close() {
        delegate.close();
    }

    /** Snapshot of every recorded read, in call-completion order. */
    public List<Range> ranges() {
        synchronized (ranges) {
            return List.copyOf(ranges);
        }
    }

    public int requestCount() {
        synchronized (ranges) {
            return ranges.size();
        }
    }

    public long bytesRead() {
        long total = 0;
        for (Range range : ranges()) {
            total += range.length();
        }
        return total;
    }

    /**
     * Highest {@code offset + length} ever read; asserts lazy tail reading. Zero-length ranges are excluded because an
     * EOF probe moves no bytes: its offset says nothing about how far into the source a read reached.
     */
    public long maxReadEnd() {
        long max = 0;
        for (Range range : ranges()) {
            if (range.length() == 0) {
                continue;
            }
            max = Math.max(max, range.offset() + range.length());
        }
        return max;
    }

    /** Total recorded bytes that fall inside {@code [lo, hi)}, counting partial overlaps. */
    public long bytesInRange(long lo, long hi) {
        long total = 0;
        for (Range range : ranges()) {
            long start = Math.max(lo, range.offset());
            long end = Math.min(hi, range.offset() + range.length());
            total += Math.max(0, end - start);
        }
        return total;
    }
}
