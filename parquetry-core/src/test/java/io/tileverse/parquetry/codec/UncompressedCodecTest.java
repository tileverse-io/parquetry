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
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.CompressionCodec;

class UncompressedCodecTest {

    @Test
    void registryLookupReturnsUncompressed() {
        Codec codec = CodecRegistry.lookup(CompressionCodec.UNCOMPRESSED);
        assertThat(codec.algorithm()).isEqualTo(CompressionCodec.UNCOMPRESSED);
    }

    @Test
    void decompressIsIdentity() throws Exception {
        Codec codec = CodecRegistry.lookup(CompressionCodec.UNCOMPRESSED);
        byte[] data = "hello".getBytes();
        ByteBuffer compressed = ByteBuffer.wrap(data);
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment out = arena.allocate(data.length);
            codec.decompress(MemorySegment.ofBuffer(compressed), out);
            assertThat(out.toArray(JAVA_BYTE)).isEqualTo(data);
        }
    }
}
