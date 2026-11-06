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
package io.tileverse.parquetry.internal.read.page;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;

import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.ParquetFormat;

/**
 * Minimal {@link InputStream} over a region of a {@link MemorySegment}, used to feed the Thrift page-header reader
 * without allocating an intermediate {@code byte[]}. Tracks its own absolute offset so callers can resume reading right
 * after the header consumes its bytes via {@link #position()}.
 */
public final class MemorySegmentInputStream extends InputStream {

    /**
     * The most bytes read for one page header. Statistics are the only part of a header without a bounded size. A
     * corrupt length inside them would read the bytes up to the end of the chunk before failing, into a heap array as
     * large. Arrow C++ refuses a page header above 16 MiB as well.
     */
    static final long MAX_PAGE_HEADER_BYTES = 16L << 20;

    private final MemorySegment segment;
    private final long start;
    private final long limit;
    private long offset;

    private MemorySegmentInputStream(MemorySegment segment, long start, long limit) {
        this.segment = segment;
        this.start = start;
        this.offset = start;
        this.limit = limit;
    }

    /**
     * A stream over the page header starting at {@code start} of {@code segment}. It ends at {@code limit}, or 16 MiB
     * after {@code start} when {@code limit} lies further.
     */
    public static MemorySegmentInputStream pageHeaderAt(MemorySegment segment, long start, long limit) {
        long end = Math.min(limit, start + MAX_PAGE_HEADER_BYTES);
        return new MemorySegmentInputStream(segment, start, end);
    }

    /**
     * Reads the page header of this stream. A header not ending within the stream is malformed. Either the bytes end
     * before the header does, or the header is longer than 16 MiB.
     */
    public PageHeader readPageHeader() {
        try {
            return ParquetFormat.readPageHeader(this);
        } catch (UncheckedIOException e) {
            throw new MalformedFileException(
                    "A page header does not end within the " + (limit - start) + " bytes available to it", e);
        }
    }

    /** Absolute offset (into the backing segment) of the next byte to be read. */
    public long position() {
        return offset;
    }

    @Override
    public int read() {
        if (offset >= limit) {
            return -1;
        }
        return segment.get(JAVA_BYTE, offset++) & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) {
        if (offset >= limit) {
            return -1;
        }
        int toRead = (int) Math.min(len, limit - offset);
        MemorySegment.copy(segment, JAVA_BYTE, offset, b, off, toRead);
        offset += toRead;
        return toRead;
    }

    @Override
    public int available() {
        return (int) Math.min(Integer.MAX_VALUE, limit - offset);
    }
}
