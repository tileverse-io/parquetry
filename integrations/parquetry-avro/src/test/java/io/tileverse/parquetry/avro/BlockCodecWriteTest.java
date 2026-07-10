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
package io.tileverse.parquetry.avro;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class BlockCodecWriteTest {

    @ParameterizedTest
    @EnumSource(BlockCodec.class)
    void compressThenDecompressRoundTrips(BlockCodec codec) {
        byte[] original = "the quick brown fox jumps over the lazy dog, repeatedly and repeatedly"
                .getBytes(StandardCharsets.UTF_8);
        MemorySegment compressed = codec.compress(MemorySegment.ofArray(original));
        MemorySegment restored = codec.decompress(compressed);
        assertThat(restored.toArray(ValueLayout.JAVA_BYTE)).isEqualTo(original);
    }

    @Test
    void snappyBlockEndsWithBigEndianCrc32OfTheUncompressedData() {
        byte[] original = "the quick brown fox jumps over the lazy dog, repeatedly and repeatedly"
                .getBytes(StandardCharsets.UTF_8);
        MemorySegment compressed = BlockCodec.SNAPPY.compress(MemorySegment.ofArray(original));

        assertThat(trailingCrc(compressed)).isEqualTo(crc32Of(original));

        MemorySegment restored = BlockCodec.SNAPPY.decompress(compressed);
        assertThat(restored.toArray(ValueLayout.JAVA_BYTE)).isEqualTo(original);
    }

    private static byte[] trailingCrc(MemorySegment compressed) {
        long size = compressed.byteSize();
        return compressed.asSlice(size - 4, 4).toArray(ValueLayout.JAVA_BYTE);
    }

    private static byte[] crc32Of(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        int value = (int) crc.getValue();
        return new byte[] {
            (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value,
        };
    }
}
