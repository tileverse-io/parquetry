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
 * PLAIN encoder for INT64: writes each value as eight little-endian bytes, contiguous.
 *
 * <p>Inverse of {@link io.tileverse.parquetry.internal.read.page.PlainInt64Decoder}.
 */
public final class PlainInt64Encoder implements Encoder<long[]> {

    @Override
    public int encode(long[] values, int n, WritableByteChannel dst) throws IOException {
        if (n == 0) {
            return 0;
        }
        int totalBytes = n * Long.BYTES;
        ByteBuffer buf = ByteBuffer.allocate(totalBytes).order(LITTLE_ENDIAN);
        for (int i = 0; i < n; i++) {
            buf.putLong(values[i]);
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
