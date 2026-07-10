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
package io.tileverse.parquetry.internal.write.page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import io.tileverse.parquetry.format.Encoding;

/**
 * BYTE_STREAM_SPLIT encoder for DOUBLE.
 *
 * <p>Inverse of {@link io.tileverse.parquetry.internal.read.page.ByteStreamSplitDoubleDecoder}. For {@code n}
 * eight-byte values the output is laid out as eight streams of {@code n} bytes each: byte 0 of every value, then byte
 * 1, ..., byte 7.
 */
public final class ByteStreamSplitDoubleEncoder implements Encoder<double[]> {

    private static final int BYTES_PER_VALUE = Double.BYTES;

    @Override
    public int encode(double[] values, int n, WritableByteChannel dst) throws IOException {
        if (n == 0) {
            return 0;
        }
        byte[] streams = new byte[n * BYTES_PER_VALUE];
        for (int i = 0; i < n; i++) {
            long bits = Double.doubleToRawLongBits(values[i]);
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
