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
package io.tileverse.parquetry.data.write.page;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import io.tileverse.parquetry.format.Encoding;

/**
 * PLAIN encoder for INT32: writes each value as four little-endian bytes, contiguous.
 *
 * <p>Inverse of {@link io.tileverse.parquetry.data.read.page.PlainInt32Decoder}: the byte stream produced here is
 * consumed by that decoder verbatim.
 */
public final class PlainInt32Encoder implements Encoder<int[]> {

    @Override
    public int encode(int[] values, int n, WritableByteChannel dst) throws IOException {
        if (n == 0) {
            return 0;
        }
        int totalBytes = n * Integer.BYTES;
        ByteBuffer buf = ByteBuffer.allocate(totalBytes).order(LITTLE_ENDIAN);
        for (int i = 0; i < n; i++) {
            buf.putInt(values[i]);
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
