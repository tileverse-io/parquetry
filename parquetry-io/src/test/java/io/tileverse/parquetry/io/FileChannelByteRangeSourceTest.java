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
package io.tileverse.parquetry.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
}
