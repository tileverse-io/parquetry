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
package io.tileverse.parquetry.arrow.ipc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IpcFramingTest {

    @Test
    void framesMessageWithContinuationLengthAndEightBytePaddingOverSequentialChannel() throws Exception {
        // Channels.newChannel(OutputStream) is NOT a GatheringByteChannel: this exercises the sequential branch.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] metadata = {1, 2, 3}; // 3 bytes -> padded to 8
        byte[] bodyBytes = {7, 8, 9, 10};

        IpcFraming.writeMessage(
                Channels.newChannel(out), ByteBuffer.wrap(metadata), List.of(MemorySegment.ofArray(bodyBytes)));

        assertFramedBytes(out.toByteArray(), metadata, bodyBytes);
    }

    @Test
    void framesMessageWithContinuationLengthAndEightBytePaddingOverGatheringChannel(@TempDir Path tempDir)
            throws Exception {
        // A FileChannel IS a GatheringByteChannel: this exercises the gathering branch.
        byte[] metadata = {1, 2, 3};
        byte[] bodyBytes = {7, 8, 9, 10};
        Path file = tempDir.resolve("frame.bin");

        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            IpcFraming.writeMessage(channel, ByteBuffer.wrap(metadata), List.of(MemorySegment.ofArray(bodyBytes)));
        }

        assertFramedBytes(Files.readAllBytes(file), metadata, bodyBytes);
    }

    @Test
    void doesNotConsumeTheCallerMetadataBuffer() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteBuffer metadata = ByteBuffer.wrap(new byte[] {1, 2, 3});

        IpcFraming.writeMessage(Channels.newChannel(out), metadata, List.of());

        assertThat(metadata.position()).isZero();
        assertThat(metadata.remaining()).isEqualTo(3);
    }

    @Test
    void endOfStreamIsContinuationThenZeroLength() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IpcFraming.writeEndOfStream(Channels.newChannel(out));
        ByteBuffer buf = ByteBuffer.wrap(out.toByteArray()).order(ByteOrder.LITTLE_ENDIAN);
        assertThat(buf.getInt()).isEqualTo(0xFFFFFFFF);
        assertThat(buf.getInt()).isZero();
        assertThat(buf.hasRemaining()).isFalse();
    }

    private static void assertFramedBytes(byte[] framed, byte[] metadata, byte[] body) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(framed).order(ByteOrder.LITTLE_ENDIAN);
        assertThat(buf.getInt()).isEqualTo(0xFFFFFFFF); // continuation
        int metaLen = buf.getInt();
        assertThat(metaLen).isEqualTo(8); // 3 padded up to a multiple of 8
        assertThat((8 + metaLen) % 8).isZero();
        byte[] meta = new byte[metaLen];
        buf.get(meta);
        assertThat(meta).startsWith(metadata);
        byte[] readBody = new byte[body.length];
        buf.get(readBody);
        assertThat(readBody).isEqualTo(body);
        assertThat(buf.hasRemaining()).isFalse();
    }
}
