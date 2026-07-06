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
package io.tileverse.parquetry.internal.read;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.runtime.DiskBudget;

import lombok.NonNull;

/**
 * Maps one temp file per spilled fetch buffer into its own {@link Arena} and hands back a {@link SegmentPool.Pooled}
 * whose {@link SegmentPool.Pooled#close() close} unmaps the segment, deletes the file, and releases the reserved
 * {@link DiskBudget} bytes. Unlike {@link BatchSpillStore}'s append-only single file, each fetch buffer is its own
 * mapped file with an independent lifecycle, which lets a buffer free its disk reservation the moment its borrower is
 * done with it.
 *
 * <p>Files live under {@code spillDir/parquetry-fetch-spill-<pid>/}. Spill files orphaned by a crashed process are
 * removed by {@link #sweepOrphans(Path)}, called once at read-runtime construction.
 */
public final class FetchSpillStore {

    private static final String SPILL_DIR_PREFIX = "parquetry-fetch-spill-";

    // Shared across every store in this JVM so two stores under the same per-process directory never pick the same
    // file name, which would fail the CREATE_NEW open with a FileAlreadyExistsException.
    private static final AtomicLong FILE_SEQUENCE = new AtomicLong();

    private final Path perProcessDir;
    private final DiskBudget diskBudget;

    public FetchSpillStore(@NonNull Path spillDir, @NonNull DiskBudget diskBudget) {
        this.perProcessDir =
                spillDir.resolve(SPILL_DIR_PREFIX + ProcessHandle.current().pid());
        this.diskBudget = diskBudget;
    }

    /**
     * Reserves {@code size} disk bytes, maps a fresh temp file of that size into its own shared arena, and returns a
     * handle owning that mapping. The caller has already decided disk is the path and ensured headroom; a failed
     * reservation here is a programming error.
     */
    SegmentPool.Pooled map(long size) {
        if (!diskBudget.tryReserve(size)) {
            throw new IllegalStateException("DiskBudget could not reserve " + size + " bytes for a fetch spill buffer");
        }
        try {
            return mapReservedFile(size);
        } catch (IOException e) {
            diskBudget.release(size);
            throw new UncheckedIOException("Failed to map fetch spill buffer of " + size + " bytes", e);
        } catch (RuntimeException e) {
            diskBudget.release(size);
            throw e;
        }
    }

    private SegmentPool.Pooled mapReservedFile(long size) throws IOException {
        Files.createDirectories(perProcessDir);
        Path file = perProcessDir.resolve(FILE_SEQUENCE.getAndIncrement() + "_" + size + ".spill");
        Arena arena = Arena.ofShared();
        try {
            MemorySegment segment = mapFile(file, size, arena);
            return new MappedFetchBuffer(segment, arena, file, size, diskBudget);
        } catch (IOException | RuntimeException e) {
            arena.close();
            Files.deleteIfExists(file);
            throw e;
        }
    }

    private MemorySegment mapFile(Path file, long size, Arena arena) throws IOException {
        try (FileChannel channel = FileChannel.open(
                file, StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            return channel.map(MapMode.READ_WRITE, 0, size, arena);
        }
    }

    /**
     * Deletes spill directories left by processes that are no longer alive, never the current process. Tolerates a
     * missing {@code spillDir} and skips entries whose trailing pid does not parse.
     */
    public static void sweepOrphans(Path spillDir) {
        if (!Files.isDirectory(spillDir)) {
            return;
        }
        try (Stream<Path> entries = Files.list(spillDir)) {
            entries.filter(FetchSpillStore::isOrphanSpillDir).forEach(FetchSpillStore::deleteTreeQuietly);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to sweep orphan fetch spill directories under " + spillDir, e);
        }
    }

    private static boolean isOrphanSpillDir(Path dir) {
        String name = dir.getFileName().toString();
        if (!name.startsWith(SPILL_DIR_PREFIX)) {
            return false;
        }
        long pid;
        try {
            pid = Long.parseLong(name.substring(SPILL_DIR_PREFIX.length()));
        } catch (NumberFormatException _) {
            return false;
        }
        if (pid == ProcessHandle.current().pid()) {
            return false;
        }
        return ProcessHandle.of(pid).isEmpty();
    }

    private static void deleteTreeQuietly(Path dir) {
        try (Stream<Path> tree = Files.walk(dir)) {
            tree.sorted(Comparator.reverseOrder()).forEach(FetchSpillStore::deleteQuietly);
        } catch (IOException _) {
            // best-effort sweep; a leftover orphan dir is retried on the next runtime start
        }
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException _) {
            // best-effort sweep; a leftover file is retried on the next runtime start
        }
    }

    /**
     * Owns one mapped temp file. Closing unmaps the segment (via the arena), deletes the file, and returns the disk
     * reservation. Idempotent for the owning borrower.
     */
    private static final class MappedFetchBuffer implements SegmentPool.Pooled {

        private final MemorySegment segment;
        private final Arena arena;
        private final Path file;
        private final long reservedBytes;
        private final DiskBudget diskBudget;
        private final AtomicBoolean closed = new AtomicBoolean();

        private MappedFetchBuffer(
                MemorySegment segment, Arena arena, Path file, long reservedBytes, DiskBudget diskBudget) {
            this.segment = segment;
            this.arena = arena;
            this.file = file;
            this.reservedBytes = reservedBytes;
            this.diskBudget = diskBudget;
        }

        @Override
        public MemorySegment segment() {
            return segment;
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            arena.close();
            deleteQuietly(file);
            diskBudget.release(reservedBytes);
        }
    }
}
