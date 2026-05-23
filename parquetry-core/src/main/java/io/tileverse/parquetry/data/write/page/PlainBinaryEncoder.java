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
 * PLAIN encoder for BYTE_ARRAY: a 4-byte little-endian length prefix followed by that many bytes per value.
 *
 * <p>Inverse of {@link io.tileverse.parquetry.data.read.page.PlainBinaryDecoder}.
 */
public final class PlainBinaryEncoder implements Encoder<byte[][]> {

    @Override
    public int encode(byte[][] values, int n, WritableByteChannel dst) throws IOException {
        int written = 0;
        byte[] prefix = new byte[Integer.BYTES];
        ByteBuffer prefixView = ByteBuffer.wrap(prefix).order(LITTLE_ENDIAN);
        for (int i = 0; i < n; i++) {
            byte[] value = values[i];
            prefixView.clear();
            prefixView.putInt(value.length);
            prefixView.flip();
            ChannelWrites.writeFully(dst, prefixView);
            ChannelWrites.writeFully(dst, ByteBuffer.wrap(value));
            written += Integer.BYTES + value.length;
        }
        return written;
    }

    @Override
    public Encoding parquetEncoding() {
        return Encoding.PLAIN;
    }
}
