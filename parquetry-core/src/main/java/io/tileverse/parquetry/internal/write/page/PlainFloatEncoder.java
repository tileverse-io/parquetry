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

import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import io.tileverse.parquetry.format.Encoding;

/**
 * PLAIN encoder for FLOAT: writes each value as four little-endian IEEE 754 bytes, contiguous.
 *
 * <p>Inverse of {@link io.tileverse.parquetry.internal.read.page.PlainFloatDecoder}. NaN and signed-zero bit patterns
 * survive encoding; the underlying {@link ByteBuffer#putFloat(float)} call uses {@link Float#floatToRawIntBits(float)}
 * semantics implicitly.
 */
public final class PlainFloatEncoder implements Encoder<float[]> {

    @Override
    public int encode(float[] values, int n, WritableByteChannel dst) throws IOException {
        if (n == 0) {
            return 0;
        }
        int totalBytes = n * Float.BYTES;
        ByteBuffer buf = ByteBuffer.allocate(totalBytes).order(LITTLE_ENDIAN);
        for (int i = 0; i < n; i++) {
            buf.putFloat(values[i]);
        }
        buf.flip();
        ChannelWrites.writeFully(dst, buf);
        return totalBytes;
    }

    @Override
    public Encoding parquetEncoding() {
        return Encoding.PLAIN;
    }
}
