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

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ByteSinkTest {

    @Test
    void appendsSegmentsAndTracksPosition(@TempDir Path dir) {
        Path file = dir.resolve("out.bin");
        byte[] first = {1, 2, 3};
        byte[] second = {4, 5};
        try (ByteSink sink = ByteSink.ofFile(file)) {
            assertThat(sink.position()).isZero();
            sink.write(MemorySegment.ofArray(first));
            assertThat(sink.position()).isEqualTo(3);
            sink.write(MemorySegment.ofArray(second));
            assertThat(sink.position()).isEqualTo(5);
        }
        try (ByteRangeSource back = ByteRangeSource.ofFile(file)) {
            MemorySegment all = MemorySegment.ofArray(new byte[(int) back.size()]);
            back.readFully(0, all);
            assertThat(all.toArray(ValueLayout.JAVA_BYTE)).containsExactly(1, 2, 3, 4, 5);
        }
    }

    @Test
    void ofFileTruncatesAnExistingFile(@TempDir Path dir) {
        Path file = dir.resolve("out.bin");
        byte[] longer = {1, 2, 3, 4, 5, 6};
        byte[] shorter = {7, 8};
        try (ByteSink sink = ByteSink.ofFile(file)) {
            sink.write(MemorySegment.ofArray(longer));
        }
        try (ByteSink sink = ByteSink.ofFile(file)) {
            sink.write(MemorySegment.ofArray(shorter));
        }
        try (ByteRangeSource back = ByteRangeSource.ofFile(file)) {
            assertThat(back.size()).isEqualTo(2);
            MemorySegment all = MemorySegment.ofArray(new byte[(int) back.size()]);
            back.readFully(0, all);
            assertThat(all.toArray(ValueLayout.JAVA_BYTE)).containsExactly(7, 8);
        }
    }

    @Test
    void ofChannelBorrowsAndReportsAbsolutePosition(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("out.bin");
        byte[] existing = {1, 2, 3, 4};
        byte[] appended = {5, 6};
        FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        try {
            channel.write(MemorySegment.ofArray(existing).asByteBuffer());
            try (ByteSink sink = ByteSink.ofChannel(channel)) {
                assertThat(sink.position()).isEqualTo(existing.length);
                sink.write(MemorySegment.ofArray(appended));
                assertThat(sink.position()).isEqualTo(existing.length + appended.length);
            }
            assertThat(channel.isOpen()).isTrue();
        } finally {
            channel.close();
        }
        try (ByteRangeSource back = ByteRangeSource.ofFile(file)) {
            MemorySegment all = MemorySegment.ofArray(new byte[(int) back.size()]);
            back.readFully(0, all);
            assertThat(all.toArray(ValueLayout.JAVA_BYTE)).containsExactly(1, 2, 3, 4, 5, 6);
        }
    }
}
