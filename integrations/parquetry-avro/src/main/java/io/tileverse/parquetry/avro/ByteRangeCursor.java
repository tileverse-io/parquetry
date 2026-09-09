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
package io.tileverse.parquetry.avro;

import java.io.EOFException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * A forward positional cursor over a {@link ByteRangeSource} for OCF container framing: the magic, the metadata map,
 * sync markers, and per-block headers. The cursor reads through a read-ahead window: each source request fetches up to
 * {@link #DEFAULT_WINDOW_SIZE} bytes from the current position, and the varints, sync markers and payloads that fit are
 * served from that window, keeping the framing of a remote file to a few range requests instead of one per byte. A
 * payload larger than the window is read straight into its target. Thread-confined; create one per read pass.
 */
final class ByteRangeCursor {

    /** Bytes fetched per source request; a file no larger than this is fetched whole by the first read. */
    static final int DEFAULT_WINDOW_SIZE = 256 * 1024;

    private static final MemorySegment EMPTY =
            MemorySegment.ofArray(new byte[0]).asReadOnly();

    private final ByteRangeSource source;
    private final int windowSize;
    private long position;

    /** The buffered source bytes: {@code window} holds the source range starting at {@code windowStart}. */
    private MemorySegment window;

    private long windowStart;

    /** The cursor's own fetch buffer, allocated on its first fetch; {@code window} is a slice of it from then on. */
    private MemorySegment fetchBuffer;

    ByteRangeCursor(ByteRangeSource source) {
        this(source, 0);
    }

    /** A cursor starting at {@code initialOffset}; the source is positional, no seek is involved. */
    ByteRangeCursor(ByteRangeSource source, long initialOffset) {
        this(source, initialOffset, DEFAULT_WINDOW_SIZE);
    }

    ByteRangeCursor(ByteRangeSource source, long initialOffset, int windowSize) {
        this(source, initialOffset, windowSize, EMPTY, 0);
    }

    private ByteRangeCursor(
            ByteRangeSource source, long initialOffset, int windowSize, MemorySegment window, long windowStart) {
        if (windowSize <= 0) {
            throw new IllegalArgumentException("Window size must be positive: " + windowSize);
        }
        this.source = source;
        this.position = initialOffset;
        this.windowSize = windowSize;
        this.window = window;
        this.windowStart = windowStart;
    }

    /**
     * A cursor starting at {@code initialOffset} whose window begins as {@code prefix}, the source's leading bytes as
     * another cursor already fetched them; reads within the prefix fetch nothing.
     */
    static ByteRangeCursor withPrefix(ByteRangeSource source, long initialOffset, MemorySegment prefix) {
        return new ByteRangeCursor(source, initialOffset, DEFAULT_WINDOW_SIZE, prefix.asReadOnly(), 0);
    }

    /**
     * The source's leading bytes as currently buffered, as a read-only copy; empty once the window has moved past
     * offset zero. A copy, because the cursor refills its fetch buffer in place on the next fetch.
     */
    MemorySegment bufferedPrefix() {
        if (windowStart != 0) {
            return EMPTY;
        }
        byte[] copy = window.toArray(ValueLayout.JAVA_BYTE);
        return MemorySegment.ofArray(copy).asReadOnly();
    }

    long position() {
        return position;
    }

    boolean hasRemaining() {
        return position < source.size();
    }

    int readRawByte() {
        if (!isBuffered(position)) {
            fetchWindowAt(position);
        }
        int value = window.get(ValueLayout.JAVA_BYTE, position - windowStart) & 0xff;
        position++;
        return value;
    }

    long readLong() {
        return Varints.decodeZigZag(readUnsignedVarLong());
    }

    MemorySegment readSegment(int length) {
        MemorySegment target = MemorySegment.ofArray(new byte[length]);
        readInto(target);
        return target;
    }

    /**
     * Reads exactly {@code target.byteSize()} bytes at the current position into {@code target}, then advances. The
     * buffered part of the range is copied from the window; a remainder at least a window long is read straight from
     * the source, a shorter one through the next window.
     */
    void readInto(MemorySegment target) {
        long length = target.byteSize();
        requireAvailable(length);
        long done = 0;
        while (done < length) {
            long remaining = length - done;
            if (!isBuffered(position)) {
                if (remaining >= windowSize) {
                    source.readFully(position, target.asSlice(done, remaining));
                    position += remaining;
                    return;
                }
                fetchWindowAt(position);
            }
            long copied = Math.min(windowEnd() - position, remaining);
            MemorySegment.copy(window, position - windowStart, target, done, copied);
            position += copied;
            done += copied;
        }
    }

    void skip(long count) {
        long target = position + count;
        if (count < 0 || target > source.size()) {
            throw new AvroFormatException("Skip past end of file to offset " + target);
        }
        position = target;
    }

    private boolean isBuffered(long offset) {
        return offset >= windowStart && offset < windowEnd();
    }

    private long windowEnd() {
        return windowStart + window.byteSize();
    }

    /** Fetches the window starting at {@code offset}: a window's worth of bytes, fewer at the end of the source. */
    private void fetchWindowAt(long offset) {
        long available = source.size() - offset;
        if (available <= 0) {
            throw new AvroFormatException("Unexpected end of file at offset " + offset);
        }
        if (fetchBuffer == null) {
            int capacity = (int) Math.min(windowSize, source.size());
            fetchBuffer = MemorySegment.ofArray(new byte[capacity]);
        }
        int length = (int) Math.min(windowSize, available);
        MemorySegment fetched = fetchBuffer.asSlice(0, length);
        source.readFully(offset, fetched);
        window = fetched;
        windowStart = offset;
    }

    private void requireAvailable(long length) {
        if (position + length > source.size()) {
            throw new UncheckedIOException(new EOFException("Reached end of source at offset " + source.size()
                    + " with " + (position + length - source.size()) + " bytes still requested"));
        }
    }

    private long readUnsignedVarLong() {
        long result = 0L;
        int shift = 0;
        while (true) {
            int b = readRawByte();
            result |= ((long) (b & 0x7f)) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
            if (shift > 63) {
                throw new AvroFormatException("Varint too long");
            }
        }
    }
}
