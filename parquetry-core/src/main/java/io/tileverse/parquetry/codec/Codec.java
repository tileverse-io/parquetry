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

import java.io.IOException;
import java.nio.ByteBuffer;

import io.tileverse.parquetry.format.enums.CompressionCodec;

/**
 * Codec SPI for Parquet compression algorithms.
 *
 * <p>Implementations are discovered via {@link java.util.ServiceLoader}. Each implementation
 * declares which {@link CompressionCodec} it handles; the {@link CodecRegistry} looks them up
 * by enum at read time.
 *
 * <p>The primary API takes both input and output buffers, letting callers manage allocations
 * (e.g., reuse a buffer pool, or wrap a region of a native {@code MemorySegment}). A
 * convenience overload allocates a heap buffer of the expected size when callers don't need
 * control.
 */
public interface Codec {

    /** The Parquet compression algorithm this codec handles. */
    CompressionCodec algorithm();

    /**
     * Decompresses {@code compressed} into {@code output}. The {@code compressed} buffer's
     * position is advanced past the consumed bytes; {@code output}'s position is advanced
     * past the bytes written. Caller must ensure {@code output.remaining() >= uncompressed
     * size}.
     *
     * @return the number of decompressed bytes written
     */
    int decompress(ByteBuffer compressed, ByteBuffer output) throws IOException;

    /**
     * Convenience: allocate a heap buffer of {@code uncompressedLength} bytes, decompress
     * into it, flip and return it. Use the two-buffer overload if you want to manage the
     * output buffer yourself.
     */
    default ByteBuffer decompress(ByteBuffer compressed, int uncompressedLength) throws IOException {
        ByteBuffer output = ByteBuffer.allocate(uncompressedLength);
        decompress(compressed, output);
        output.flip();
        return output;
    }
}
