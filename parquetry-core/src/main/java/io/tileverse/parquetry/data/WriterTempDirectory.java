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
package io.tileverse.parquetry.data;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.stream.Stream;

/**
 * The writer's private working directory backing the per-column accumulators: an owner-only subdirectory created under
 * the caller-configured temp dir, deleted (best effort) when the writer closes on either the success or failure path.
 */
final class WriterTempDirectory {

    private WriterTempDirectory() {}

    /**
     * Creates a private working directory inside the caller-supplied {@link WriteOptions#tempDir()}.
     *
     * <p>The caller's {@code tempDir} (typically {@code $java.io.tmpdir}) must already exist, be a directory, and be
     * writable; the writer never modifies its permissions or contents beyond the per-run subdirectory it creates here.
     * The subdirectory is named with a random suffix to avoid collisions between concurrent writers, and on POSIX
     * filesystems is created atomically with owner-only permissions (rwx------) so other users on the same machine
     * cannot read intermediate column-chunk bytes. On non-POSIX filesystems the underlying platform's per-user temp
     * conventions apply.
     *
     * <p>The directory is deleted when the writer closes (success or failure path).
     */
    static Path createTempDir(WriteOptions options) {
        Path parent = options.tempDir();
        if (!Files.exists(parent)) {
            throw new ParquetWriteException("WriteOptions.tempDir does not exist: " + parent);
        }
        if (!Files.isDirectory(parent)) {
            throw new ParquetWriteException("WriteOptions.tempDir is not a directory: " + parent);
        }
        if (!Files.isWritable(parent)) {
            throw new ParquetWriteException("WriteOptions.tempDir is not writable: " + parent);
        }
        try {
            return Files.createTempDirectory(parent, "parquetry-write-", ownerOnlyAttributes(parent.getFileSystem()));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create writer temp directory under " + parent, e);
        }
    }

    private static FileAttribute<?>[] ownerOnlyAttributes(FileSystem fs) {
        if (!fs.supportedFileAttributeViews().contains("posix")) {
            return new FileAttribute<?>[0];
        }
        EnumSet<PosixFilePermission> ownerRwx = EnumSet.of(
                PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE);
        return new FileAttribute<?>[] {PosixFilePermissions.asFileAttribute(ownerRwx)};
    }

    static void deleteTempDirQuietly(Path dir) {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException _) {
                    /* best effort */
                }
            });
        } catch (IOException _) {
            /* best effort */
        }
    }
}
