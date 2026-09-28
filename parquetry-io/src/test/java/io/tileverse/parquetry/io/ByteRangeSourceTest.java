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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

class ByteRangeSourceTest {

    private static final byte[] CONTENT = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);

    private Path write(Path dir) throws Exception {
        Path file = dir.resolve("data.bin");
        Files.write(file, CONTENT);
        return file;
    }

    private static byte[] readToArray(ByteRangeSource source, long offset, int length) {
        byte[] dst = new byte[length];
        int read = source.read(offset, MemorySegment.ofArray(dst));
        byte[] trimmed = new byte[Math.max(read, 0)];
        System.arraycopy(dst, 0, trimmed, 0, trimmed.length);
        return trimmed;
    }

    @Test
    void sizeReportsFileLength(@TempDir Path dir) throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(write(dir))) {
            assertThat(source.size()).isEqualTo(CONTENT.length);
        }
    }

    @Test
    void positionalReadReturnsRequestedBytes(@TempDir Path dir) throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(write(dir))) {
            assertThat(readToArray(source, 4, 5)).isEqualTo("45678".getBytes(StandardCharsets.US_ASCII));
        }
    }

    @Test
    void readPastEndIsShort(@TempDir Path dir) throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(write(dir))) {
            assertThat(readToArray(source, 12, 8)).isEqualTo("CDEF".getBytes(StandardCharsets.US_ASCII));
        }
    }

    @Test
    void readAtOrAfterEndReturnsMinusOne(@TempDir Path dir) throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(write(dir))) {
            assertThat(source.read(CONTENT.length, MemorySegment.ofArray(new byte[4])))
                    .isEqualTo(-1);
            assertThat(source.read(CONTENT.length + 10, MemorySegment.ofArray(new byte[4])))
                    .isEqualTo(-1);
        }
    }

    @Test
    void readFullyThrowsOnShortRead(@TempDir Path dir) throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(write(dir))) {
            MemorySegment dst = MemorySegment.ofArray(new byte[8]);
            assertThatThrownBy(() -> source.readFully(12, dst)).isInstanceOf(UncheckedIOException.class);
        }
    }

    @Test
    void readFullyFillsExactly(@TempDir Path dir) throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(write(dir))) {
            byte[] dst = new byte[6];
            source.readFully(2, MemorySegment.ofArray(dst));
            assertThat(dst).isEqualTo("234567".getBytes(StandardCharsets.US_ASCII));
        }
    }

    @Test
    void concurrentReadsAreConsistent(@TempDir Path dir) throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(write(dir))) {
            List<Callable<Boolean>> tasks = new ArrayList<>();
            for (int i = 0; i < CONTENT.length; i++) {
                int offset = i;
                tasks.add(() -> readToArray(source, offset, 1)[0] == CONTENT[offset]);
            }
            try (ExecutorService pool = Executors.newFixedThreadPool(8)) {
                List<Future<Boolean>> futures = pool.invokeAll(tasks);
                for (Future<Boolean> future : futures) {
                    assertThat(future.get()).isTrue();
                }
            }
        }
    }

    @Test
    void ofFileClosesItsChannel(@TempDir Path dir) throws Exception {
        ByteRangeSource source = ByteRangeSource.ofFile(write(dir));
        source.close();
        MemorySegment dst = MemorySegment.ofArray(new byte[1]);
        assertThatThrownBy(() -> source.read(0, dst))
                .isInstanceOf(UncheckedIOException.class)
                .cause()
                .isInstanceOf(ClosedChannelException.class);
    }

    @Test
    void sizeStaysKnownAfterClose(@TempDir Path dir) throws Exception {
        ByteRangeSource source = ByteRangeSource.ofFile(write(dir));
        source.close();
        assertThat(source.size()).isEqualTo(CONTENT.length);
    }

    @Test
    void ofFileOfAMissingFileFailsAsAMissingFile(@TempDir Path dir) {
        Path missing = dir.resolve("missing.bin");
        assertThatThrownBy(() -> ByteRangeSource.ofFile(missing))
                .isInstanceOf(UncheckedIOException.class)
                .cause()
                .isInstanceOf(NoSuchFileException.class);
    }

    @Test
    void ofFileOfADirectoryIsRejected(@TempDir Path dir) {
        assertThatThrownBy(() -> ByteRangeSource.ofFile(dir)).isInstanceOf(IllegalArgumentException.class);
    }

    /** Opening only inspects the file; the channel opens on the first read, and a denied read fails there. */
    @Test
    void ofFileOfAnUnreadableFileOpensAndFailsOnTheFirstRead(@TempDir Path dir) throws Exception {
        Path file = write(dir);
        assumeTrue(denyFileReads(file), "needs POSIX permissions and a non-root user");
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            MemorySegment dst = MemorySegment.ofArray(new byte[4]);
            assertThatThrownBy(() -> source.read(0, dst))
                    .isInstanceOf(UncheckedIOException.class)
                    .cause()
                    .isInstanceOf(AccessDeniedException.class);
        } finally {
            restoreFileReads(file);
        }
    }

    @Test
    void ofChannelDoesNotCloseBorrowedChannel(@TempDir Path dir) throws Exception {
        try (FileChannel channel = FileChannel.open(write(dir), StandardOpenOption.READ)) {
            ByteRangeSource source = ByteRangeSource.ofChannel(channel);
            source.close();
            assertThat(channel.isOpen()).isTrue();
        }
    }

    @Test
    void zeroLengthReadReturnsZero(@TempDir Path dir) throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(write(dir))) {
            assertThat(source.read(0, MemorySegment.ofArray(new byte[0]))).isZero();
            assertThat(source.read(CONTENT.length, MemorySegment.ofArray(new byte[0])))
                    .isZero();
        }
    }

    @Test
    void readRejectsReadOnlyDestination(@TempDir Path dir) throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(write(dir))) {
            MemorySegment readOnly = MemorySegment.ofArray(new byte[4]).asReadOnly();
            assertThatThrownBy(() -> source.read(0, readOnly)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void readRejectsNegativeOffset(@TempDir Path dir) throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(write(dir))) {
            MemorySegment dst = MemorySegment.ofArray(new byte[4]);
            assertThatThrownBy(() -> source.read(-1, dst)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    @DisabledOnOs(
            value = OS.WINDOWS,
            disabledReason = "Windows applies the '..' to the spelling before following the link, which opens one"
                    + " file for both spellings; the collision pinned by this test cannot arise there")
    void namesTwoFilesApartWhenOneSpellingLeavesASymbolicLink(@TempDir Path dir) throws Exception {
        Path root = symlinkedTree(dir);
        Path directPath = root.resolve("a/x.parquet");
        Path pastTheLinkPath = root.resolve("a/link/../x.parquet");

        // Both spellings normalize to one textual path. Only the filesystem tells the two files apart.
        assertThat(pastTheLinkPath.normalize()).isEqualTo(directPath.normalize());

        try (ByteRangeSource direct = ByteRangeSource.ofFile(directPath);
                ByteRangeSource pastTheLink = ByteRangeSource.ofFile(pastTheLinkPath)) {
            assertThat(contentOf(direct)).isNotEqualTo(contentOf(pastTheLink));
            assertThat(direct.size())
                    .as("equal lengths leave the name as the whole of the identity")
                    .isEqualTo(pastTheLink.size());
            assertThat(direct.sourceIdentifier()).isNotEqualTo(pastTheLink.sourceIdentifier());
        }
    }

    @Test
    void namesOneFileTheSameThroughASymbolicLinkAndDirectly(@TempDir Path dir) throws Exception {
        Path root = symlinkedTree(dir);

        try (ByteRangeSource throughLink = ByteRangeSource.ofFile(root.resolve("a/link/x.parquet"));
                ByteRangeSource direct = ByteRangeSource.ofFile(root.resolve("b/inner/x.parquet"))) {
            // two unnamed sources compare equal, which would pass this test without either one being named
            assertThat(throughLink.sourceIdentifier()).isPresent();
            assertThat(direct.sourceIdentifier()).isPresent();
            assertThat(throughLink.sourceIdentifier()).isEqualTo(direct.sourceIdentifier());
        }
    }

    /**
     * Three same-length files under {@code a/x.parquet}, {@code b/x.parquet} and {@code b/inner/x.parquet}, with
     * {@code a/link} pointing at {@code b/inner}. Spelled {@code a/link/../x.parquet}, the file opened is
     * {@code b/x.parquet} on a filesystem that follows the link before applying the {@code ..}; Windows applies the
     * {@code ..} to the spelling first and opens {@code a/x.parquet}, which is why the collision test is disabled
     * there.
     */
    private static Path symlinkedTree(Path dir) throws Exception {
        Path root = Files.createDirectory(dir.resolve("tree"));
        writeFile(root.resolve("a/x.parquet"), "AAAAAAAAAAAAAAAA");
        writeFile(root.resolve("b/x.parquet"), "BBBBBBBBBBBBBBBB");
        writeFile(root.resolve("b/inner/x.parquet"), "CCCCCCCCCCCCCCCC");
        createSymbolicLink(root.resolve("a/link"), Path.of("../b/inner"));
        return root;
    }

    private static void writeFile(Path file, String content) throws Exception {
        Files.createDirectories(file.getParent());
        Files.writeString(file, content, StandardCharsets.US_ASCII);
    }

    /** Aborts the test on a platform or filesystem that refuses to create symbolic links. */
    private static void createSymbolicLink(Path link, Path target) {
        try {
            Files.createSymbolicLink(link, target);
        } catch (UnsupportedOperationException | IOException e) {
            abort("this filesystem does not create symbolic links: " + e);
        }
    }

    private static String contentOf(ByteRangeSource source) {
        MemorySegment dst = Arena.ofAuto().allocate(source.size());
        source.readFully(0, dst);
        return new String(dst.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.US_ASCII);
    }

    /**
     * Removes every permission from {@code file} and reports whether that stops a read of it: false on a filesystem
     * without POSIX permissions or for a user not bound by permissions, such as root, with the file readable again.
     */
    private static boolean denyFileReads(Path file) throws IOException {
        if (!FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
            return false;
        }
        Files.setPosixFilePermissions(file, Set.of());
        boolean denied = false;
        try (SeekableByteChannel channel = Files.newByteChannel(file, StandardOpenOption.READ)) {
            channel.size();
        } catch (AccessDeniedException _) {
            denied = true;
        } finally {
            if (!denied) {
                restoreFileReads(file);
            }
        }
        return denied;
    }

    private static void restoreFileReads(Path file) throws IOException {
        Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rw-r--r--"));
    }
}
