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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.OptionalLong;

import io.tileverse.storage.RangeReader;

/**
 * Adapts a tileverse-storage {@link RangeReader} to a {@link ByteRangeSource}. The adapter either borrows the reader,
 * leaving {@link #close()} a no-op for a reader shared and cached across reads, or owns it, closing the reader with the
 * source for a reader handed out fresh by a {@code Storage}.
 */
final class RangeReaderByteRangeSource implements ByteRangeSource {

    private final RangeReader reader;
    private final boolean ownsReader;
    private final long size;

    /**
     * @throws IllegalStateException if the reader cannot report its size; an owned reader is closed before the throw
     */
    RangeReaderByteRangeSource(RangeReader reader, boolean ownsReader) {
        this.reader = reader;
        this.ownsReader = ownsReader;
        this.size = sizeOf(reader, ownsReader);
    }

    private static long sizeOf(RangeReader reader, boolean ownsReader) {
        OptionalLong knownSize = reader.size();
        if (knownSize.isPresent()) {
            return knownSize.getAsLong();
        }
        if (ownsReader) {
            closeReader(reader);
        }
        throw new IllegalStateException("RangeReader cannot determine source size: " + reader.getSourceIdentifier());
    }

    @Override
    public long size() {
        return size;
    }

    /**
     * The reader's own source identifier, typically the object's URI. A reader that declines to name its source leaves
     * this adapter unnamed rather than failing the read.
     */
    @Override
    public Optional<String> sourceIdentifier() {
        return Optional.ofNullable(reader.getSourceIdentifier());
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
        if (ownsReader) {
            closeReader(reader);
        }
    }

    private static void closeReader(RangeReader reader) {
        try {
            reader.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close RangeReader " + reader.getSourceIdentifier(), e);
        }
    }
}
