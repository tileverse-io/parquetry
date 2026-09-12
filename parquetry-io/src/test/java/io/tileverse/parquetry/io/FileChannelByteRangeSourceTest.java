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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

class FileChannelByteRangeSourceTest {

    @TempDir
    Path tempDir;

    private static final byte[] BYTES = "ABCDEFGH".getBytes(StandardCharsets.US_ASCII);

    @Test
    void reopensAndRecoversAfterTheLiveChannelGoesStale() throws Exception {
        Path file = tempDir.resolve("data.bin");
        Files.write(file, BYTES);

        List<FileChannel> opened = new ArrayList<>();
        FileChannelByteRangeSource.ChannelOpener opener = () -> {
            FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
            opened.add(channel);
            return channel;
        };

        try (FileChannelByteRangeSource source = FileChannelByteRangeSource.owning(file, opener)) {
            opened.get(0).close(); // the live handle goes stale under the next read

            MemorySegment dst = Arena.ofAuto().allocate(BYTES.length);
            int read = source.read(0, dst);

            assertThat(read).isEqualTo(BYTES.length);
            assertThat(opened).hasSize(2); // reopened exactly once
            assertThat(dst.toArray(ValueLayout.JAVA_BYTE)).isEqualTo(BYTES);
        }
    }

    @Test
    void givesUpAfterASingleReopenAttempt() throws Exception {
        Path file = tempDir.resolve("data.bin");
        Files.write(file, BYTES);

        List<FileChannel> opened = new ArrayList<>();
        AtomicInteger opens = new AtomicInteger();
        FileChannelByteRangeSource.ChannelOpener opener = () -> {
            FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
            opened.add(channel);
            if (opens.getAndIncrement() >= 1) {
                channel.close(); // the reopened channel is dead too
            }
            return channel;
        };

        try (FileChannelByteRangeSource source = FileChannelByteRangeSource.owning(file, opener)) {
            opened.get(0).close();
            MemorySegment dst = Arena.ofAuto().allocate(BYTES.length);

            assertThatThrownBy(() -> source.read(0, dst)).isInstanceOf(UncheckedIOException.class);
            assertThat(opened).hasSize(2); // one reopen attempt, then it gives up
        }
    }

    @Test
    @DisabledOnOs(
            value = OS.WINDOWS,
            disabledReason = "Windows applies the '..' to the spelling before following the link, which opens one"
                    + " file for both spellings; the collision pinned by this test cannot arise there")
    void namesTwoFilesApartWhenOneSpellingLeavesASymbolicLink() throws Exception {
        Path root = symlinkedTree();
        Path directPath = root.resolve("a/x.parquet");
        Path pastTheLinkPath = root.resolve("a/link/../x.parquet");

        // Both spellings normalize to one textual path. Only the filesystem tells the two files apart.
        assertThat(pastTheLinkPath.normalize()).isEqualTo(directPath.normalize());

        try (FileChannelByteRangeSource direct = FileChannelByteRangeSource.owning(directPath);
                FileChannelByteRangeSource pastTheLink = FileChannelByteRangeSource.owning(pastTheLinkPath)) {
            assertThat(contentOf(direct)).isNotEqualTo(contentOf(pastTheLink));
            assertThat(direct.size())
                    .as("equal lengths leave the name as the whole of the identity")
                    .isEqualTo(pastTheLink.size());
            assertThat(direct.sourceIdentifier()).isNotEqualTo(pastTheLink.sourceIdentifier());
        }
    }

    @Test
    void namesOneFileTheSameThroughASymbolicLinkAndDirectly() throws Exception {
        Path root = symlinkedTree();

        try (FileChannelByteRangeSource throughLink =
                        FileChannelByteRangeSource.owning(root.resolve("a/link/x.parquet"));
                FileChannelByteRangeSource direct =
                        FileChannelByteRangeSource.owning(root.resolve("b/inner/x.parquet"))) {
            // two unnamed sources compare equal, which would pass this test without either one being named
            assertThat(throughLink.sourceIdentifier()).isPresent();
            assertThat(direct.sourceIdentifier()).isPresent();
            assertThat(throughLink.sourceIdentifier()).isEqualTo(direct.sourceIdentifier());
        }
    }

    @Test
    void leavesABorrowedChannelUnnamed() throws Exception {
        Path file = tempDir.resolve("data.bin");
        Files.write(file, BYTES);

        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
                FileChannelByteRangeSource source = FileChannelByteRangeSource.borrowing(channel)) {
            assertThat(source.sourceIdentifier()).isEmpty();
        }
    }

    /**
     * Three same-length files under {@code a/x.parquet}, {@code b/x.parquet} and {@code b/inner/x.parquet}, with
     * {@code a/link} pointing at {@code b/inner}. Spelled {@code a/link/../x.parquet}, the file opened is
     * {@code b/x.parquet} on a filesystem that follows the link before applying the {@code ..}; Windows applies the
     * {@code ..} to the spelling first and opens {@code a/x.parquet}, which is why the collision test is disabled
     * there.
     */
    private Path symlinkedTree() throws Exception {
        Path root = Files.createDirectory(tempDir.resolve("tree"));
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
}
