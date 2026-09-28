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

import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BorrowedChannelByteRangeSourceTest {

    private static final byte[] BYTES = "ABCDEFGH".getBytes(StandardCharsets.US_ASCII);

    @Test
    void readsPositionallyThroughTheBorrowedChannel(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("data.bin");
        Files.write(file, BYTES);

        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
                ByteRangeSource source = ByteRangeSource.ofChannel(channel)) {
            assertThat(source.size()).isEqualTo(BYTES.length);

            byte[] dst = new byte[3];
            assertThat(source.read(5, MemorySegment.ofArray(dst))).isEqualTo(3);
            assertThat(dst).isEqualTo("FGH".getBytes(StandardCharsets.US_ASCII));

            assertThat(source.read(BYTES.length, MemorySegment.ofArray(new byte[1])))
                    .isEqualTo(-1);
        }
    }

    @Test
    void leavesABorrowedChannelUnnamed(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("data.bin");
        Files.write(file, BYTES);

        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
                ByteRangeSource source = ByteRangeSource.ofChannel(channel)) {
            assertThat(source.sourceIdentifier()).isEmpty();
        }
    }

    /** The source cannot reopen a channel it does not own; a closed channel fails the read. */
    @Test
    void aClosedBorrowedChannelFailsTheRead(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("data.bin");
        Files.write(file, BYTES);
        FileChannel channel = FileChannel.open(file, StandardOpenOption.READ);
        ByteRangeSource source = ByteRangeSource.ofChannel(channel);
        channel.close();
        MemorySegment dst = MemorySegment.ofArray(new byte[4]);

        assertThatThrownBy(() -> source.read(0, dst))
                .isInstanceOf(UncheckedIOException.class)
                .cause()
                .isInstanceOf(ClosedChannelException.class);
    }
}
