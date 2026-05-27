/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.tileverse;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.OptionalLong;

import io.tileverse.storage.RangeReader;

import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * Adapts a tileverse-storage {@link RangeReader} to a parquetry {@link ByteRangeSource}. BORROWS the reader: the
 * adapter's {@link #close()} is a no-op, and the caller closes the {@code RangeReader} after the last read. A
 * {@code RangeReader} is typically shared and cached across reads, which is why the adapter never closes it.
 */
final class RangeReaderByteRangeSource implements ByteRangeSource {

    private final RangeReader reader;
    private final long size;

    RangeReaderByteRangeSource(RangeReader reader) {
        this.reader = reader;
        OptionalLong knownSize = reader.size();
        if (knownSize.isEmpty()) {
            throw new IllegalStateException(
                    "RangeReader cannot determine source size: " + reader.getSourceIdentifier());
        }
        this.size = knownSize.getAsLong();
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public int read(long offset, MemorySegment dst) {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0, got " + offset);
        }
        long length = dst.byteSize();
        if (length == 0) {
            return 0;
        }
        if (length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("dst.byteSize() must be <= Integer.MAX_VALUE, got " + length);
        }
        if (offset >= size) {
            return -1;
        }
        // RangeReader.readRange may return a short count for cloud backends; loop to honor the
        // ByteRangeSource guarantee that an in-bounds read is fully satisfied.
        int requested = (int) length;
        int total = 0;
        while (total < requested) {
            ByteBuffer target = dst.asSlice(total, (long) requested - total).asByteBuffer();
            int read = reader.readRange(offset + total, requested - total, target);
            if (read <= 0) {
                break;
            }
            total += read;
        }
        return total == 0 ? -1 : total;
    }

    @Override
    public void close() {
        // borrows the RangeReader; the caller owns and closes it
    }
}
