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
package io.tileverse.parquetry.arrow;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Writes Arrow IPC "encapsulated message" framing to a {@link WritableByteChannel}: a continuation word, a
 * little-endian metadata length, the flatbuffer metadata padded to an 8-byte boundary, then the message body segments.
 * Also writes the end-of-stream marker.
 *
 * <p>Arrow IPC spec: continuation = 0xFFFFFFFF, then int32 metadata length (LE), then metadata bytes padded so that (8
 * + metadataLength) is a multiple of 8, then body bytes. EOS = continuation followed by int32 0.
 *
 * <p>The body segments are written without copying: each is presented to the channel via
 * {@link MemorySegment#asByteBuffer()}. When the channel is a {@link GatheringByteChannel} the prefix, padded metadata,
 * and body are written in one gathering call; otherwise they are written sequentially. Each body segment is already
 * 8-byte padded by the codec, keeping consecutive writes Arrow-aligned.
 */
final class IpcFraming {

    private static final int CONTINUATION = 0xFFFFFFFF;
    private static final int ALIGNMENT = 8;

    private IpcFraming() {}

    static void writeMessage(WritableByteChannel channel, ByteBuffer metadata, List<MemorySegment> body)
            throws IOException {
        int metadataLength = metadata.remaining();
        int paddedMetadataLength = align(ALIGNMENT + metadataLength) - ALIGNMENT;

        ByteBuffer prefix = ByteBuffer.allocate(2 * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        prefix.putInt(CONTINUATION).putInt(paddedMetadataLength).flip();

        List<ByteBuffer> framed = new ArrayList<>(body.size() + 2);
        framed.add(prefix);
        framed.add(paddedMetadata(metadata, paddedMetadataLength));
        for (MemorySegment segment : body) {
            framed.add(segment.asByteBuffer());
        }
        writeAll(channel, framed);
    }

    static void writeEndOfStream(WritableByteChannel channel) throws IOException {
        ByteBuffer eos = ByteBuffer.allocate(2 * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        eos.putInt(CONTINUATION).putInt(0).flip();
        writeAll(channel, List.of(eos));
    }

    private static ByteBuffer paddedMetadata(ByteBuffer metadata, int paddedLength) {
        if (paddedLength == metadata.remaining()) {
            return metadata.duplicate();
        }
        ByteBuffer padded = ByteBuffer.allocate(paddedLength);
        padded.put(metadata.duplicate()).rewind();
        return padded;
    }

    private static void writeAll(WritableByteChannel channel, List<ByteBuffer> buffers) throws IOException {
        if (channel instanceof GatheringByteChannel gathering) {
            ByteBuffer[] array = buffers.toArray(new ByteBuffer[0]);
            long remaining = 0;
            for (ByteBuffer buffer : array) {
                remaining += buffer.remaining();
            }
            while (remaining > 0) {
                remaining -= gathering.write(array);
            }
            return;
        }
        for (ByteBuffer buffer : buffers) {
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
        }
    }

    static int align(int value) {
        int remainder = value % ALIGNMENT;
        return remainder == 0 ? value : value + (ALIGNMENT - remainder);
    }
}
