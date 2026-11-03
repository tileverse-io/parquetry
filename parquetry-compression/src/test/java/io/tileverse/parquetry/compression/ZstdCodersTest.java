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
package io.tileverse.parquetry.compression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;

import io.airlift.compress.v3.zstd.ZstdCompressor;
import io.airlift.compress.v3.zstd.ZstdDecompressor;
import io.airlift.compress.v3.zstd.ZstdJavaCompressor;
import io.airlift.compress.v3.zstd.ZstdJavaDecompressor;
import io.airlift.compress.v3.zstd.ZstdNativeDecompressor;

class ZstdCodersTest {

    private static final byte[] PAYLOAD =
            "the quick brown fox jumps over the lazy dog ".repeat(64).getBytes();

    @Test
    void sharesCodersOnOneThread() {
        assumeTrue(ZstdNativeDecompressor.isEnabled(), "no native zstd library on this platform");
        ZstdCoders coders = ZstdCoders.sharedNative(4);

        assertThat(coders.decompressor()).isSameAs(coders.decompressor());
        assertThat(coders.compressor(3)).isSameAs(coders.compressor(3));
        assertThat(coders.compressor(3)).as("a compressor per level").isNotSameAs(coders.compressor(1));
    }

    @Test
    void pureJavaCodersPerCall() {
        ZstdCoders coders = ZstdCoders.pureJava();

        ZstdDecompressor decompressor = coders.decompressor();
        ZstdCompressor compressor = coders.compressor(3);
        assertThat(decompressor).isInstanceOf(ZstdJavaDecompressor.class).isNotSameAs(coders.decompressor());
        assertThat(compressor).isInstanceOf(ZstdJavaCompressor.class).isNotSameAs(coders.compressor(3));
    }

    @Test
    void pureJavaIgnoresTheLevel() {
        ZstdCoders coders = ZstdCoders.pureJava();

        assertThat(roundTrip(coders, 9, PAYLOAD)).isEqualTo(PAYLOAD);
    }

    // With one stripe for eight threads, calls collide on its coders and allocate temporary contexts.
    @Test
    void collidingCallsRoundTrip() throws Exception {
        assumeTrue(ZstdNativeDecompressor.isEnabled(), "no native zstd library on this platform");
        ZstdCoders coders = ZstdCoders.sharedNative(1);

        List<Future<?>> tasks = new ArrayList<>();
        try (ExecutorService threads = Executors.newFixedThreadPool(8)) {
            for (int task = 0; task < 8; task++) {
                byte[] payload = ("task " + task + " ").repeat(200).getBytes();
                tasks.add(threads.submit(() -> roundTripRepeatedly(coders, payload)));
            }
            for (Future<?> task : tasks) {
                task.get();
            }
        }
    }

    // With one stripe, the failed calls and the round trip go through the same coders.
    @Test
    void survivesFailedCalls() {
        assumeTrue(ZstdNativeDecompressor.isEnabled(), "no native zstd library on this platform");
        ZstdCoders coders = ZstdCoders.sharedNative(1);
        ZstdDecompressor decompressor = coders.decompressor();
        ZstdCompressor compressor = coders.compressor(Codec.Zstd.DEFAULT_LEVEL);

        byte[] garbage = new byte[64];
        Arrays.fill(garbage, (byte) 0xFF);
        MemorySegment corrupt = MemorySegment.ofArray(garbage);
        MemorySegment output = MemorySegment.ofArray(new byte[PAYLOAD.length]);
        assertThatThrownBy(() -> decompressor.decompress(corrupt, output)).isInstanceOf(RuntimeException.class);

        MemorySegment payload = MemorySegment.ofArray(PAYLOAD);
        MemorySegment tiny = MemorySegment.ofArray(new byte[1]);
        assertThatThrownBy(() -> compressor.compress(payload, tiny)).isInstanceOf(RuntimeException.class);

        MemorySegment compressed = MemorySegment.ofArray(new byte[compressor.maxCompressedLength(PAYLOAD.length)]);
        int compressedLength = compressor.compress(payload, compressed);
        MemorySegment frame = compressed.asSlice(0, compressedLength);
        assertThatThrownBy(() -> decompressor.decompress(frame, tiny)).isInstanceOf(RuntimeException.class);

        assertThat(roundTrip(coders, Codec.Zstd.DEFAULT_LEVEL, PAYLOAD)).isEqualTo(PAYLOAD);
    }

    @Test
    void halvesStripesAboveDefault() {
        assertThat(ZstdCoders.compressorStripes(128, 1)).isEqualTo(128);
        assertThat(ZstdCoders.compressorStripes(128, Codec.Zstd.DEFAULT_LEVEL)).isEqualTo(128);
        assertThat(ZstdCoders.compressorStripes(128, 4)).isEqualTo(64);
        assertThat(ZstdCoders.compressorStripes(128, 9)).isEqualTo(2);
        assertThat(ZstdCoders.compressorStripes(128, 22)).isEqualTo(1);
        assertThat(ZstdCoders.compressorStripes(8, 6)).isEqualTo(1);
    }

    @Test
    void rejectsStripesNotPowerOfTwo() {
        assertThatThrownBy(() -> ZstdCoders.sharedNative(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ZstdCoders.sharedNative(6)).isInstanceOf(IllegalArgumentException.class);
    }

    private static void roundTripRepeatedly(ZstdCoders coders, byte[] payload) {
        for (int i = 0; i < 100; i++) {
            assertThat(roundTrip(coders, Codec.Zstd.DEFAULT_LEVEL, payload)).isEqualTo(payload);
        }
    }

    private static byte[] roundTrip(ZstdCoders coders, int level, byte[] payload) {
        ZstdCompressor compressor = coders.compressor(level);
        MemorySegment compressed = MemorySegment.ofArray(new byte[compressor.maxCompressedLength(payload.length)]);
        int compressedLength = compressor.compress(MemorySegment.ofArray(payload), compressed);

        byte[] back = new byte[payload.length];
        ZstdDecompressor decompressor = coders.decompressor();
        int length = decompressor.decompress(compressed.asSlice(0, compressedLength), MemorySegment.ofArray(back));
        assertThat(length).isEqualTo(payload.length);
        return back;
    }
}
