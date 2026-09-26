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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * A {@link ByteRangeSource} that delegates to a backing source and records the offset and length of every read. Tests
 * ask it whether a byte span of the file was ever touched, which proves that a skipped row group or a dropped page was
 * never fetched.
 *
 * <p>Reads arrive from several threads: a prefetching read runs its speculative fetches on virtual threads while the
 * consumer thread fetches the current row group inline. Recording, clearing and querying therefore all hold this
 * object's monitor, which both keeps the record list intact and publishes every recorded read to the asserting thread.
 * The lock is off the measured path: nothing here is timed.
 */
public final class RecordingByteRangeSource implements ByteRangeSource {

    private final ByteRangeSource delegate;
    private final List<long[]> reads = new ArrayList<>();

    public RecordingByteRangeSource(ByteRangeSource delegate) {
        this.delegate = delegate;
    }

    @Override
    public long size() {
        return delegate.size();
    }

    @Override
    public int read(long offset, MemorySegment dst) {
        noteRead(offset, dst.byteSize());
        return delegate.read(offset, dst);
    }

    @Override
    public void close() {
        delegate.close();
    }

    /** Forgets every read recorded so far, which lets one test assert over several reads of the same source. */
    public synchronized void reset() {
        reads.clear();
    }

    /** Whether any recorded read overlaps the byte span {@code [start, end)}. */
    public synchronized boolean readAnyByteIn(long start, long end) {
        for (long[] read : reads) {
            long readStart = read[0];
            long readEnd = read[0] + read[1];
            if (readStart < end && start < readEnd) {
                return true;
            }
        }
        return false;
    }

    /**
     * How many distinct bytes of the span {@code [start, end)} were touched by the recorded reads. Bytes fetched twice
     * count once, which makes the number comparable between two reads of the same file.
     */
    public synchronized long bytesReadIn(long start, long end) {
        List<long[]> touched = clipTo(start, end);
        touched.sort(Comparator.comparingLong(span -> span[0]));
        long total = 0;
        long coveredTo = start;
        for (long[] span : touched) {
            long from = Math.max(span[0], coveredTo);
            if (from < span[1]) {
                total += span[1] - from;
                coveredTo = span[1];
            }
        }
        return total;
    }

    private List<long[]> clipTo(long start, long end) {
        List<long[]> clipped = new ArrayList<>();
        for (long[] read : reads) {
            long readStart = Math.max(start, read[0]);
            long readEnd = Math.min(end, read[0] + read[1]);
            if (readStart < readEnd) {
                clipped.add(new long[] {readStart, readEnd});
            }
        }
        return clipped;
    }

    private synchronized void noteRead(long offset, long length) {
        reads.add(new long[] {offset, length});
    }
}
