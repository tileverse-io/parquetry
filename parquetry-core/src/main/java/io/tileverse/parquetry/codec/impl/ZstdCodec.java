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

import java.io.IOException;
import java.nio.ByteBuffer;

import io.tileverse.parquetry.codec.Codec;
import io.tileverse.parquetry.format.enums.CompressionCodec;

import io.airlift.compress.v3.zstd.ZstdDecompressor;

public final class ZstdCodec implements Codec {

    private final ZstdDecompressor decompressor = ZstdDecompressor.create();

    @Override
    public CompressionCodec algorithm() {
        return CompressionCodec.ZSTD;
    }

    @Override
    public int decompress(ByteBuffer compressed, ByteBuffer output) throws IOException {
        byte[] in = ByteBuffers.toArray(compressed);
        byte[] out = new byte[output.remaining()];
        int written = decompressor.decompress(in, 0, in.length, out, 0, out.length);
        output.put(out, 0, written);
        return written;
    }
}
