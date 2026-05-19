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

import java.lang.foreign.MemorySegment;

import io.tileverse.parquetry.format.enums.CompressionCodec;

import io.airlift.compress.v3.lz4.Lz4Decompressor;

/**
 * Parquet LZ4_RAW codec. Uses aircompressor's {@link Lz4Decompressor}, which handles the raw LZ4 block format (no frame
 * header) that Parquet's LZ4_RAW encoding uses. Each {@link #decompress(MemorySegment, MemorySegment)} call constructs
 * a fresh decompressor so the codec instance itself stays stateless and safe to share across virtual threads (see
 * {@code ConcurrencyMode#FAN_OUT_ONLY}).
 */
public final class Lz4RawCodec implements Codec {

    @Override
    public CompressionCodec algorithm() {
        return CompressionCodec.LZ4_RAW;
    }

    @Override
    public int decompress(MemorySegment compressed, MemorySegment output) {
        return Lz4Decompressor.create().decompress(compressed, output);
    }
}
