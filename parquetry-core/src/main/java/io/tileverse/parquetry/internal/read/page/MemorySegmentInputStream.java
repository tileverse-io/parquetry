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
package io.tileverse.parquetry.internal.read.page;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.io.InputStream;
import java.lang.foreign.MemorySegment;

/**
 * Minimal {@link InputStream} over a region of a {@link MemorySegment}, used to feed the Thrift page-header reader
 * without allocating an intermediate {@code byte[]}. Tracks its own absolute offset so callers can resume reading right
 * after the header consumes its bytes via {@link #position()}.
 */
public final class MemorySegmentInputStream extends InputStream {

    private final MemorySegment segment;
    private final long limit;
    private long offset;

    public MemorySegmentInputStream(MemorySegment segment, long start, long limit) {
        this.segment = segment;
        this.offset = start;
        this.limit = limit;
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
