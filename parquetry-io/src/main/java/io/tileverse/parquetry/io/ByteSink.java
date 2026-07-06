/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Objects;

/**
 * A forward, append-only byte sink: parquetry's single write dependency, the write counterpart to
 * {@link ByteRangeSource}. Bytes are appended in call order; there is deliberately no positional write and no seek,
 * matching the forward-only shape of the container formats parquetry emits.
 */
public interface ByteSink extends AutoCloseable {

    /** Appends the whole of {@code src} to the end of the sink. */
    void write(MemorySegment src);

    /** Total bytes appended so far; equals the final byte length once {@link #close()} runs. */
    long position();

    /**
     * Appends {@code count} bytes from {@code src} starting at {@code srcPosition} to the end of this sink. The default
     * memory-maps the source region and appends it; {@link #position()} advances by {@code count}. File-backed sinks
     * override this to perform a zero-copy channel-to-channel transfer instead.
     */
    default void transferFrom(FileChannel src, long srcPosition, long count) {
        if (count == 0L) {
            return;
        }
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment region = src.map(FileChannel.MapMode.READ_ONLY, srcPosition, count, arena);
            write(region);
        } catch (IOException e) {
            throw new UncheckedIOException("transferFrom failed", e);
        }
    }

    /** Narrowed: no checked exception (matches parquetry's RuntimeException-only public API). */
    @Override
    void close();

    /**
     * Opens a {@link FileChannel} over {@code path} for writing and OWNS it; {@link #close()} closes the channel. An
     * existing file is truncated.
     *
     * @throws UncheckedIOException if the file cannot be opened
     */
    static ByteSink ofFile(Path path) {
        return FileChannelByteSink.owning(Objects.requireNonNull(path, "path"));
    }

    /**
     * Wraps an existing {@link FileChannel}; BORROWS it - {@link #close()} does not close the channel. Bytes are
     * appended at the channel's current position onward. Wrapping performs no I/O and cannot fail.
     */
    static ByteSink ofChannel(FileChannel channel) {
        return FileChannelByteSink.borrowing(Objects.requireNonNull(channel, "channel"));
    }

    /**
     * Wraps an {@link OutputStream} and OWNS it; {@link #close()} closes the stream, which commits an object-storage
     * upload whose backing stream commits on close. This is the write counterpart used when the destination is an
     * {@link OutputStream}, whether a local file stream or a tileverse-storage object stream.
     */
    static ByteSink ofOutputStream(OutputStream out) {
        return new OutputStreamByteSink(Objects.requireNonNull(out, "out"));
    }
}
