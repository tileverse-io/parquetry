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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.airlift.compress.v3.zstd.ZstdCompressor;
import io.airlift.compress.v3.zstd.ZstdDecompressor;
import io.airlift.compress.v3.zstd.ZstdJavaCompressor;
import io.airlift.compress.v3.zstd.ZstdJavaDecompressor;
import io.airlift.compress.v3.zstd.ZstdNativeCompressor;
import io.airlift.compress.v3.zstd.ZstdNativeDecompressor;

/**
 * The zstd compressors and decompressors of {@link Codec.Zstd}.
 *
 * <p>With the native zstd library, calls share a fixed set of coders. The coders are split in stripes, and a call takes
 * the coder of the stripe picked by the id of its thread. A native coder keeps one zstd context and reuses it from call
 * to call. When two threads use one coder at the same time, the second one allocates a temporary context. The coder
 * keeps one of the two contexts and frees the other when its call ends.
 *
 * <p>The shared contexts stay allocated for the life of the process. A stripe holds at most one decompression context
 * of about 100 KB, and one compression context per level, about 1 MB at the default level.
 *
 * <p>Without the native library, as on Windows and Intel Macs, each call gets a new pure-Java coder. The pure-Java
 * decompressor keeps scratch state in its fields and cannot serve two calls at once.
 */
final class ZstdCoders {

    /**
     * The coders used by this process. They are shared native coders when the native library loads, and pure-Java
     * coders otherwise.
     */
    static final ZstdCoders SHARED = forThisPlatform();

    private final boolean nativeLibrary;
    private final int stripes;
    private final ZstdDecompressor[] decompressors;
    private final Map<Integer, ZstdCompressor[]> compressorsByLevel = new ConcurrentHashMap<>();

    private ZstdCoders(boolean nativeLibrary, int stripes) {
        this.nativeLibrary = nativeLibrary;
        this.stripes = stripes;
        this.decompressors = new ZstdDecompressor[nativeLibrary ? stripes : 0];
        for (int i = 0; i < decompressors.length; i++) {
            decompressors[i] = new ZstdNativeDecompressor();
        }
    }

    /**
     * Shared native coders in four stripes per processor, rounded down to a power of two, or pure-Java coders when the
     * native library does not load on this platform.
     */
    private static ZstdCoders forThisPlatform() {
        if (!ZstdNativeDecompressor.isEnabled() || !ZstdNativeCompressor.isEnabled()) {
            return pureJava();
        }
        int processors = Runtime.getRuntime().availableProcessors();
        return sharedNative(Integer.highestOneBit(processors * 4));
    }

    /**
     * Native coders shared in stripes. A coder allocates its context on the first call of its stripe.
     *
     * @param stripes the number of shared decompressors, a power of two
     * @throws IllegalArgumentException if {@code stripes} is not a power of two
     */
    static ZstdCoders sharedNative(int stripes) {
        if (Integer.bitCount(stripes) != 1) {
            throw new IllegalArgumentException("stripes must be a power of two, got " + stripes);
        }
        return new ZstdCoders(true, stripes);
    }

    /** Pure-Java coders, a new one per call. */
    static ZstdCoders pureJava() {
        return new ZstdCoders(false, 0);
    }

    /** The shared native decompressor of the stripe of the calling thread, or a new pure-Java decompressor. */
    ZstdDecompressor decompressor() {
        if (!nativeLibrary) {
            return new ZstdJavaDecompressor();
        }
        return decompressors[stripeOfThisThread(decompressors.length)];
    }

    /**
     * The shared native compressor at {@code level} of the stripe of the calling thread, or a new pure-Java compressor.
     *
     * <p>The pure-Java compressor accepts no level but its default, and it compresses at that level regardless of
     * {@code level}. The level changes the compression ratio, not the wire format: a payload written at the default
     * level reads back the same on every platform.
     */
    ZstdCompressor compressor(int level) {
        if (!nativeLibrary) {
            return new ZstdJavaCompressor();
        }
        ZstdCompressor[] atLevel = compressorsByLevel.computeIfAbsent(level, this::nativeCompressors);
        return atLevel[stripeOfThisThread(atLevel.length)];
    }

    private ZstdCompressor[] nativeCompressors(int level) {
        ZstdCompressor[] atLevel = new ZstdCompressor[compressorStripes(stripes, level)];
        for (int i = 0; i < atLevel.length; i++) {
            atLevel[i] = new ZstdNativeCompressor(level);
        }
        return atLevel;
    }

    /**
     * The number of compressors shared at {@code level}. A compression context grows with the level and with the size
     * of its input. For a 1 MB input, it holds about 1 MB at the default level, 2.5 MB at level 4 and more than 12 MB
     * from level 9. Each level above the default halves the number of shared compressors. The memory held by idle
     * contexts then stays about the same or shrinks as the level grows.
     *
     * <p>Fewer compressors make two calls on one compressor more likely, and the second call allocates a temporary
     * context. Compression is slow enough at those levels for that allocation to cost little.
     */
    static int compressorStripes(int stripes, int level) {
        int halvings = Math.max(0, level - Codec.Zstd.DEFAULT_LEVEL);
        return Math.max(1, stripes >> halvings);
    }

    private static int stripeOfThisThread(int stripeCount) {
        long threadId = Thread.currentThread().threadId();
        return (int) (threadId & (stripeCount - 1));
    }
}
