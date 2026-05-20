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
package io.tileverse.parquetry.codec;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;

import org.brotli.dec.BrotliInputStream;

import io.tileverse.parquetry.format.CompressionCodec;

/**
 * Brotli codec. The {@code org.brotli:dec} library is stream/byte-array based, so the compressed input is copied into a
 * {@code byte[]} at the boundary; the decompressed output is written directly into the destination
 * {@link MemorySegment} in 8KB chunks.
 */
public final class BrotliCodec implements Codec {

    @Override
    public CompressionCodec algorithm() {
        return CompressionCodec.BROTLI;
    }

    @Override
    public int decompress(MemorySegment compressed, MemorySegment output) throws IOException {
        byte[] in = compressed.toArray(JAVA_BYTE);
        try (BrotliInputStream br = new BrotliInputStream(new ByteArrayInputStream(in))) {
            return streamToSegment(br, output);
        }
    }

    private static int streamToSegment(InputStream in, MemorySegment output) throws IOException {
        byte[] buf = new byte[8192];
        int written = 0;
        while (true) {
            int n = in.read(buf);
            if (n < 0) {
                break;
            }
            MemorySegment.copy(buf, 0, output, JAVA_BYTE, written, n);
            written += n;
        }
        return written;
    }
}
