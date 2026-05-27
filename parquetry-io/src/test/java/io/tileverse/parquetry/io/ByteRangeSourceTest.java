/*
 * Copyright (c) 2026 Tileverse.io
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
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;
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
        assertThatThrownBy(() -> source.read(0, dst)).isInstanceOf(UncheckedIOException.class);
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
}
