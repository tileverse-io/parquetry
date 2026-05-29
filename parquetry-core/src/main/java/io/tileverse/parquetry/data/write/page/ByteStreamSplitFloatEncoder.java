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
package io.tileverse.parquetry.data.write.page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import io.tileverse.parquetry.format.Encoding;

/**
 * BYTE_STREAM_SPLIT encoder for FLOAT.
 *
 * <p>Inverse of {@link io.tileverse.parquetry.data.read.page.ByteStreamSplitFloatDecoder}. For {@code n} four-byte
 * values the output is laid out as four streams of {@code n} bytes each: byte 0 of every value, then byte 1, byte 2,
 * byte 3. Values are interpreted as little-endian IEEE 754 bit patterns.
 */
public final class ByteStreamSplitFloatEncoder implements Encoder<float[]> {

    private static final int BYTES_PER_VALUE = Float.BYTES;

    @Override
    public int encode(float[] values, int n, WritableByteChannel dst) throws IOException {
        if (n == 0) {
            return 0;
        }
        byte[] streams = new byte[n * BYTES_PER_VALUE];
        for (int i = 0; i < n; i++) {
            int bits = Float.floatToRawIntBits(values[i]);
            for (int s = 0; s < BYTES_PER_VALUE; s++) {
                streams[s * n + i] = (byte) ((bits >>> (s * 8)) & 0xff);
            }
        }
        ChannelWrites.writeFully(dst, ByteBuffer.wrap(streams));
        return streams.length;
    }

    @Override
    public Encoding parquetEncoding() {
        return Encoding.BYTE_STREAM_SPLIT;
    }
}
