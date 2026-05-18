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
package io.tileverse.parquetry.codec.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;

import io.tileverse.parquetry.codec.Codec;
import io.tileverse.parquetry.format.enums.CompressionCodec;

public final class GZipCodec implements Codec {

    @Override
    public CompressionCodec algorithm() {
        return CompressionCodec.GZIP;
    }

    @Override
    public int decompress(ByteBuffer compressed, ByteBuffer output) throws IOException {
        byte[] in = ByteBuffers.toArray(compressed);
        try (GZIPInputStream gz = new GZIPInputStream(new ByteArrayInputStream(in))) {
            int written = 0;
            byte[] buf = new byte[8192];
            while (true) {
                int n = gz.read(buf);
                if (n < 0) {
                    break;
                }
                output.put(buf, 0, n);
                written += n;
            }
            return written;
        }
    }
}
