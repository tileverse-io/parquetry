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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Writes Arrow IPC "encapsulated message" framing: a continuation word, a little-endian metadata length, the flatbuffer
 * metadata padded to an 8-byte boundary, then the message body. Also writes the end-of-stream marker.
 *
 * <p>Arrow IPC spec: continuation = 0xFFFFFFFF, then int32 metadata length (LE), then metadata bytes padded so that (8
 * + metadataLength) is a multiple of 8, then body bytes. EOS = continuation followed by int32 0.
 */
final class IpcFraming {

    private static final int CONTINUATION = 0xFFFFFFFF;
    private static final int ALIGNMENT = 8;

    private IpcFraming() {}

    static void writeMessage(OutputStream out, ByteBuffer metadata, byte[] body) throws IOException {
        int metadataLength = metadata.remaining();
        int paddedMetadataLength = align(ALIGNMENT + metadataLength) - ALIGNMENT;

        out.write(intLittleEndian(CONTINUATION));
        out.write(intLittleEndian(paddedMetadataLength));
        writeBuffer(out, metadata);
        writePadding(out, paddedMetadataLength - metadataLength);
        out.write(body);
    }

    static void writeEndOfStream(OutputStream out) throws IOException {
        out.write(intLittleEndian(CONTINUATION));
        out.write(intLittleEndian(0));
    }

    static int align(int value) {
        int remainder = value % ALIGNMENT;
        return remainder == 0 ? value : value + (ALIGNMENT - remainder);
    }

    private static void writeBuffer(OutputStream out, ByteBuffer buffer) throws IOException {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        out.write(bytes);
    }

    private static void writePadding(OutputStream out, int count) throws IOException {
        for (int i = 0; i < count; i++) {
            out.write(0);
        }
    }

    private static byte[] intLittleEndian(int value) {
        return ByteBuffer.allocate(Integer.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putInt(value)
                .array();
    }
}
