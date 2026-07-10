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
 * PLAIN encoder for BOOLEAN: bit-packed, LSB-first, eight values per byte.
 *
 * <p>Inverse of {@link io.tileverse.parquetry.internal.read.page.PlainBooleanDecoder}. Bit 0 of byte 0 is value 0, bit
 * 1 of byte 0 is value 1, etc. Trailing bits in the final byte are zero-padded.
 */
public final class PlainBooleanEncoder implements Encoder<boolean[]> {

    @Override
    public int encode(boolean[] values, int n, WritableByteChannel dst) throws IOException {
        if (n == 0) {
            return 0;
        }
        int byteCount = (n + 7) >>> 3;
        byte[] packed = new byte[byteCount];
        for (int i = 0; i < n; i++) {
            if (values[i]) {
                packed[i >>> 3] |= (byte) (1 << (i & 7));
            }
        }
        ChannelWrites.writeFully(dst, ByteBuffer.wrap(packed));
        return byteCount;
    }

    @Override
    public Encoding parquetEncoding() {
        return Encoding.PLAIN;
    }
}
