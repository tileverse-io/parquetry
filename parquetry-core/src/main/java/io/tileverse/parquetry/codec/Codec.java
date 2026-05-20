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
import java.lang.foreign.MemorySegment;

import io.tileverse.parquetry.format.CompressionCodec;

/**
 * Codec SPI for Parquet compression algorithms.
 *
 * <p>Implementations are discovered via {@link java.util.ServiceLoader}. Each implementation declares which
 * {@link CompressionCodec} it handles; the {@link CodecRegistry} looks them up by enum at read time.
 *
 * <p>The entry point takes a pair of {@link MemorySegment}s so the engine can hand zero-copy views of native memory
 * (e.g. a pooled output region or a slice of a column-chunk buffer).
 */
public interface Codec {

    /** The Parquet compression algorithm this codec handles. */
    CompressionCodec algorithm();

    /**
     * Decompresses the contents of {@code compressed} into {@code output}. The {@code output} segment must be at least
     * as large as the expected uncompressed payload. Returns the number of bytes written.
     */
    int decompress(MemorySegment compressed, MemorySegment output) throws IOException;
}
