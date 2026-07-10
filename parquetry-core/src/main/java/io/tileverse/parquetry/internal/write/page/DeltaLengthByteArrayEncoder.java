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
 * DELTA_LENGTH_BYTE_ARRAY encoder.
 *
 * <p>Wire format (parquet-format Encodings.md): the value lengths encoded as a DELTA_BINARY_PACKED int32 run, followed
 * by the concatenated payload bytes. Inverse of
 * {@link io.tileverse.parquetry.internal.read.page.DeltaLengthByteArrayDecoder}.
 */
public final class DeltaLengthByteArrayEncoder implements Encoder<byte[][]> {

    @Override
    public int encode(byte[][] values, int n, WritableByteChannel dst) throws IOException {
        long[] lengths = new long[n];
        for (int i = 0; i < n; i++) {
            lengths[i] = values[i].length;
        }
        int written = DeltaBinaryPackedWriter.write(lengths, n, dst);
        for (int i = 0; i < n; i++) {
            ChannelWrites.writeFully(dst, ByteBuffer.wrap(values[i]));
            written += values[i].length;
        }
        return written;
    }

    @Override
    public Encoding parquetEncoding() {
        return Encoding.DELTA_LENGTH_BYTE_ARRAY;
    }
}
