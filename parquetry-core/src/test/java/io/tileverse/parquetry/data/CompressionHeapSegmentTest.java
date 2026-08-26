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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Round-trips each compressing codec through HEAP-backed {@link MemorySegment}s, as opposed to the confined off-heap
 * arenas exercised by {@code CompressionTest}. The page writer compresses a heap view of the already-encoded value
 * bytes; this pins that the engine backends accept heap-backed segments on both the compress and decompress ends.
 */
class CompressionHeapSegmentTest {

    static Stream<Compression> roundTripCodecs() {
        return Stream.of(
                new Compression.Uncompressed(),
                new Compression.Snappy(),
                new Compression.Gzip(),
                new Compression.Lz4Raw(),
                new Compression.Zstd(3),
                new Compression.Lzo());
    }

    @ParameterizedTest
    @MethodSource("roundTripCodecs")
    void heapSegmentCompressRoundTrips(Compression codec) throws Exception {
        byte[] original = new byte[4096];
        for (int i = 0; i < original.length; i++) {
            original[i] = (byte) ((i * 31) ^ (i >>> 3));
        }
        MemorySegment src = MemorySegment.ofArray(original);
        int maxLen = Math.toIntExact(codec.maxCompressedLength(original.length));
        MemorySegment compressed = MemorySegment.ofArray(new byte[maxLen]);
        int written = codec.compress(src, compressed);

        MemorySegment back = MemorySegment.ofArray(new byte[original.length]);
        int produced = codec.decompress(compressed.asSlice(0L, written), back);

        assertThat(produced).isEqualTo(original.length);
        assertThat(back.toArray(ValueLayout.JAVA_BYTE)).isEqualTo(original);
    }

    static Stream<Compression> decodeOnlyCodecs() {
        return Stream.of(new Compression.Brotli(), new Compression.Lz4Hadoop());
    }

    @ParameterizedTest
    @MethodSource("decodeOnlyCodecs")
    void decodeOnlyCodecsRefuseCompressionOfHeapSegments(Compression codec) {
        MemorySegment src = MemorySegment.ofArray(new byte[] {1, 2, 3});
        assertThatThrownBy(() -> codec.compress(src, src)).isInstanceOf(UnsupportedOperationException.class);
    }
}
