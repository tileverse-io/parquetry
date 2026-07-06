/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
 * PLAIN encoder for FIXED_LEN_BYTE_ARRAY: contiguous fixed-width payloads with no length prefix.
 *
 * <p>Inverse of {@link io.tileverse.parquetry.internal.read.page.PlainFixedLenBinaryDecoder}. The fixed width comes
 * from the schema's {@code typeLength}; every value passed to {@link #encode} must match it exactly.
 */
public record PlainFixedLenBinaryEncoder(int length) implements Encoder<byte[][]> {

    public PlainFixedLenBinaryEncoder {
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative; got " + length);
        }
    }

    @Override
    public int encode(byte[][] values, int n, WritableByteChannel dst) throws IOException {
        for (int i = 0; i < n; i++) {
            byte[] value = values[i];
            if (value.length != length) {
                throw new IllegalArgumentException("FIXED_LEN_BYTE_ARRAY value at index " + i + " has length "
                        + value.length + " but encoder is configured for " + length);
            }
            ChannelWrites.writeFully(dst, ByteBuffer.wrap(value));
        }
        return n * length;
    }

    @Override
    public Encoding parquetEncoding() {
        return Encoding.PLAIN;
    }
}
