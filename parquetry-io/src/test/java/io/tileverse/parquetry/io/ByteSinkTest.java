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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
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

    @Test
    void ofOutputStreamAppendsSegmentsAndTracksPosition() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] first = {1, 2, 3};
        byte[] second = {4, 5};
        try (ByteSink sink = ByteSink.ofOutputStream(out)) {
            assertThat(sink.position()).isZero();
            sink.write(MemorySegment.ofArray(first));
            assertThat(sink.position()).isEqualTo(3);
            sink.write(MemorySegment.ofArray(second));
            assertThat(sink.position()).isEqualTo(5);
        }
        assertThat(out.toByteArray()).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    void ofOutputStreamCloseClosesTheUnderlyingStream() {
        ClosingFlagOutputStream out = new ClosingFlagOutputStream();
        try (ByteSink sink = ByteSink.ofOutputStream(out)) {
            sink.write(MemorySegment.ofArray(new byte[] {1, 2}));
            assertThat(out.closed).isFalse();
        }
        assertThat(out.closed).isTrue();
    }

    @Test
    void transferFromDefaultOverOutputStreamSink(@TempDir Path dir) throws IOException {
        Path source = dir.resolve("source.bin");
        byte[] content = {10, 11, 12, 13, 14, 15, 16, 17};
        Files.write(source, content);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (FileChannel src = FileChannel.open(source, StandardOpenOption.READ);
                ByteSink sink = ByteSink.ofOutputStream(out)) {
            sink.transferFrom(src, 0, content.length);
            assertThat(sink.position()).isEqualTo(content.length);
        }
        assertThat(out.toByteArray()).containsExactly(10, 11, 12, 13, 14, 15, 16, 17);
    }

    @Test
    void transferFromDefaultOverOutputStreamSinkPartialRegion(@TempDir Path dir) throws IOException {
        Path source = dir.resolve("source.bin");
        byte[] content = {10, 11, 12, 13, 14, 15, 16, 17};
        Files.write(source, content);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (FileChannel src = FileChannel.open(source, StandardOpenOption.READ);
                ByteSink sink = ByteSink.ofOutputStream(out)) {
            sink.transferFrom(src, 2, 3);
            assertThat(sink.position()).isEqualTo(3);
        }
        assertThat(out.toByteArray()).containsExactly(12, 13, 14);
    }

    @Test
    void transferFromOverrideOverFileChannelSink(@TempDir Path dir) throws IOException {
        Path source = dir.resolve("source.bin");
        Path dest = dir.resolve("dest.bin");
        byte[] content = {20, 21, 22, 23, 24, 25};
        Files.write(source, content);
        try (FileChannel src = FileChannel.open(source, StandardOpenOption.READ);
                ByteSink sink = ByteSink.ofFile(dest)) {
            sink.transferFrom(src, 1, 4);
            assertThat(sink.position()).isEqualTo(4);
        }
        assertThat(Files.readAllBytes(dest)).containsExactly(21, 22, 23, 24);
    }

    @Test
    void transferFromZeroCountIsANoOp(@TempDir Path dir) throws IOException {
        Path source = dir.resolve("source.bin");
        Files.write(source, new byte[] {1, 2, 3});
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (FileChannel src = FileChannel.open(source, StandardOpenOption.READ);
                ByteSink sink = ByteSink.ofOutputStream(out)) {
            sink.transferFrom(src, 0, 0);
            assertThat(sink.position()).isZero();
        }
        assertThat(out.toByteArray()).isEmpty();
    }

    @Test
    void ofOutputStreamCloseIsIdempotent() {
        ClosingFlagOutputStream out = new ClosingFlagOutputStream();
        ByteSink sink = ByteSink.ofOutputStream(out);
        sink.write(MemorySegment.ofArray(new byte[] {1, 2}));
        sink.close();
        sink.close();
        assertThat(out.closed).isTrue();
        assertThat(out.closeCount).isEqualTo(1);
    }

    @Test
    void transferFromDefaultDrainsLargeRegionBeyondChannelBuffer(@TempDir Path dir) throws IOException {
        int size = 100 * 1024;
        byte[] content = new byte[size];
        for (int i = 0; i < size; i++) {
            content[i] = (byte) i;
        }
        Path source = dir.resolve("large.bin");
        Files.write(source, content);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (FileChannel src = FileChannel.open(source, StandardOpenOption.READ);
                ByteSink sink = ByteSink.ofOutputStream(out)) {
            sink.transferFrom(src, 0, size);
            assertThat(sink.position()).isEqualTo(size);
        }
        byte[] drained = out.toByteArray();
        assertThat(drained).hasSize(size);
        assertThat(drained[0]).isEqualTo(content[0]);
        assertThat(drained[size / 2]).isEqualTo(content[size / 2]);
        assertThat(drained[size - 1]).isEqualTo(content[size - 1]);
    }

    private static final class ClosingFlagOutputStream extends OutputStream {
        private boolean closed;
        private int closeCount;

        @Override
        public void write(int b) {
            // The test only inspects close(); discarding bytes keeps it focused.
        }

        @Override
        public void close() {
            closed = true;
            closeCount++;
        }
    }
}
