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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import io.tileverse.parquetry.format.Encoding;

/**
 * DELTA_BYTE_ARRAY encoder.
 *
 * <p>Wire format (parquet-format Encodings.md):
 *
 * <pre>
 *   prefix-lengths (DELTA_BINARY_PACKED i32, N entries)
 *   suffix-lengths (DELTA_BINARY_PACKED i32, N entries)
 *   suffix-bytes   (concatenated)
 * </pre>
 *
 * <p>Each value {@code V[i]} is split into a leading prefix shared with {@code V[i-1]} plus a fresh suffix: {@code V[i]
 * = V[i-1][0..P[i]] + S[i]}. The first prefix length is zero. Inverse of
 * {@link io.tileverse.parquetry.data.read.page.DeltaByteArrayDecoder}.
 */
public final class DeltaByteArrayEncoder implements Encoder<byte[][]> {

    @Override
    public int encode(byte[][] values, int n, WritableByteChannel dst) throws IOException {
        long[] prefixLengths = new long[n];
        long[] suffixLengths = new long[n];
        byte[] previous = new byte[0];
        for (int i = 0; i < n; i++) {
            int common = commonPrefix(previous, values[i]);
            prefixLengths[i] = common;
            suffixLengths[i] = (long) values[i].length - common;
            previous = values[i];
        }

        // DeltaByteArrayDecoder explicitly drains the DELTA_BINARY_PACKED padding via skip() between the prefix and
        // suffix length streams to land on the suffix-bytes section, so we have to emit the full padded block bytes.
        int written = DeltaBinaryPackedWriter.writeFullyPadded(prefixLengths, n, dst);
        written += DeltaBinaryPackedWriter.writeFullyPadded(suffixLengths, n, dst);
        for (int i = 0; i < n; i++) {
            int common = (int) prefixLengths[i];
            int suffixLength = values[i].length - common;
            ChannelWrites.writeFully(dst, ByteBuffer.wrap(values[i], common, suffixLength));
            written += suffixLength;
        }
        return written;
    }

    private static int commonPrefix(byte[] a, byte[] b) {
        int limit = Math.min(a.length, b.length);
        int i = 0;
        while (i < limit && a[i] == b[i]) {
            i++;
        }
        return i;
    }

    @Override
    public Encoding parquetEncoding() {
        return Encoding.DELTA_BYTE_ARRAY;
    }
}
