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
package io.tileverse.parquetry.internal.read;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

import io.tileverse.parquetry.arrow.columnar.BatchArrowLayout;
import io.tileverse.parquetry.arrow.columnar.EncodedBatch;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.observe.SpillAccumulator;
import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

/**
 * Spills decoded batches for one row group to an append-only temp file and reads them back on demand. Mirrors the write
 * path's per-column-chunk temp-file staging: one file per row group, created lazily on the first spill and deleted on
 * close. Each spilled batch is the Arrow-buffer image produced by {@link EncodedBatchSerializer}; the
 * {@link DiskBudget} is reserved for the serialized size on spill and released when the batch is restored or the store
 * closes.
 */
final class BatchSpillStore implements AutoCloseable {

    private final Path spillDir;
    private final DiskBudget diskBudget;
    private final ParquetSchema projectedSchema;
    private final SpillAccumulator spillAccumulator;
    private final ReentrantLock lock = new ReentrantLock();

    private Path file;
    private FileChannel channel;
    private long appendOffset;
    private boolean closed;

    BatchSpillStore(
            @NonNull Path spillDir,
            @NonNull DiskBudget diskBudget,
            @NonNull ParquetSchema projectedSchema,
            @NonNull SpillAccumulator spillAccumulator) {
        this.spillDir = spillDir;
        this.diskBudget = diskBudget;
        this.projectedSchema = projectedSchema;
        this.spillAccumulator = spillAccumulator;
    }

    /**
     * Serializes {@code batch}, reserves its size from the disk budget, and appends it to the spill file. Returns the
     * handle, or empty when the disk budget has no headroom (the caller then parks). The batch's heap is the caller's
     * to free after a present result.
     */
    Optional<SpillHandle> trySpill(ParquetRecordBatch batch) {
        EncodedBatch encoded = BatchArrowLayout.encode(batch);
        MemorySegment image = EncodedBatchSerializer.serialize(encoded);
        long length = image.byteSize();
        if (!diskBudget.tryReserve(length)) {
            spillAccumulator.recordSpillRejectedDiskFull();
            return Optional.empty();
        }
        lock.lock();
        try {
            ensureOpen();
            long offset = appendOffset;
            writeFully(image, offset);
            appendOffset += length;
            spillAccumulator.recordSpill(length);
            return Optional.of(new SpillHandle(offset, length));
        } catch (IOException e) {
            diskBudget.release(length);
            throw new UncheckedIOException("Failed to spill decoded batch", e);
        } finally {
            lock.unlock();
        }
    }

    /** Reads {@code handle}'s bytes back, releases its disk reservation, and rebuilds a heap batch. */
    ParquetRecordBatch restore(SpillHandle handle) {
        long start = System.nanoTime();
        byte[] image = new byte[(int) handle.length()];
        lock.lock();
        try {
            readFully(image, handle.offset());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to restore spilled batch", e);
        } finally {
            lock.unlock();
        }
        diskBudget.release(handle.length());
        EncodedBatch encoded = EncodedBatchSerializer.deserialize(MemorySegment.ofArray(image));
        ParquetRecordBatch restored = BatchArrowLayout.decode(encoded, projectedSchema);
        spillAccumulator.recordRestore(System.nanoTime() - start);
        return restored;
    }

    /** Releases an unconsumed spilled batch's disk reservation without reading it (the file is removed on close). */
    void releaseUnconsumed(SpillHandle handle) {
        diskBudget.release(handle.length());
    }

    @Override
    public void close() {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            closeChannelQuietly();
            deleteFileQuietly();
        } finally {
            lock.unlock();
        }
    }

    private void ensureOpen() throws IOException {
        if (file != null) {
            return;
        }
        Files.createDirectories(spillDir);
        file = Files.createTempFile(spillDir, "spill-", ".tmp");
        channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    private void writeFully(MemorySegment image, long offset) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(image.toArray(ValueLayout.JAVA_BYTE));
        long position = offset;
        while (buffer.hasRemaining()) {
            int written = channel.write(buffer, position);
            if (written == 0) {
                throw new IOException("FileChannel.write made no progress");
            }
            position += written;
        }
    }

    private void readFully(byte[] image, long offset) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(image);
        long position = offset;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer, position);
            if (read < 0) {
                throw new IOException("Spill file truncated at offset " + position);
            }
            position += read;
        }
    }

    private void closeChannelQuietly() {
        if (channel == null) {
            return;
        }
        try {
            channel.close();
        } catch (IOException _) {
            // best-effort; the file is deleted next
        }
    }

    private void deleteFileQuietly() {
        if (file == null) {
            return;
        }
        try {
            Files.deleteIfExists(file);
        } catch (IOException _) {
            // best-effort; a leaked temp file is cleaned by the OS temp sweep
        }
    }
}
