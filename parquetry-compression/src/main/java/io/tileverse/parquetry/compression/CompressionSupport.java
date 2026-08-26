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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;

/** Package-private helpers shared by {@link Codec} record implementations. */
final class CompressionSupport {

    private CompressionSupport() {}

    private static final int DEFLATE_HEADER_TRAILER_ALLOWANCE = 64;
    private static final int DEFLATE_BLOCK_OVERHEAD_GRANULARITY = 16 * 1024;
    private static final long DEFLATE_MIN_OUTPUT_SIZE = 32L;

    static int intExact(long uncompressedLength) {
        if (uncompressedLength < 0 || uncompressedLength > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("uncompressedLength out of range: " + uncompressedLength);
        }
        return (int) uncompressedLength;
    }

    /**
     * Conservative compressed-size bound for DEFLATE-based codecs, mirroring {@code java.util.zip.Deflater}: the source
     * length, rounded up to a 16 KiB boundary at 5 bytes of block overhead each, plus a 64-byte header/trailer
     * allowance, with a 32-byte floor for empty/tiny payloads.
     */
    static long deflateBound(long uncompressedLength) {
        // Java's Deflater accepts int-sized inputs only; capping here avoids overflow in the rounding arithmetic
        // below when uncompressedLength is near Long.MAX_VALUE.
        if (uncompressedLength < 0 || uncompressedLength > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("uncompressedLength out of range: " + uncompressedLength);
        }
        long blocks =
                (uncompressedLength + DEFLATE_BLOCK_OVERHEAD_GRANULARITY - 1) / DEFLATE_BLOCK_OVERHEAD_GRANULARITY;
        long overheadFromBlocks = blocks * 5L;
        long total = uncompressedLength + overheadFromBlocks + DEFLATE_HEADER_TRAILER_ALLOWANCE;
        return Math.max(total, DEFLATE_MIN_OUTPUT_SIZE);
    }

    /** Copies the first {@code max} bytes of {@code src} (or all of it when shorter) into a fresh array. */
    static byte[] prefix(MemorySegment src, int max) {
        int length = (int) Math.min(src.byteSize(), max);
        byte[] out = new byte[length];
        MemorySegment.copy(src, JAVA_BYTE, 0, out, 0, length);
        return out;
    }

    /**
     * Runs an aircompressor segment-to-segment decode, tolerating a read-only source without copying when the engine
     * accepts one. The optimized decoder reads a read-only segment directly; the pure-Java fallback reaches the base
     * address through {@code Unsafe} and refuses a read-only segment, and which decoder aircompressor picks varies by
     * codec and platform (the Windows runner hits the fallback for snappy and zstd; macOS hits it for lzo). Only on
     * that refusal, and only for a read-only source, is the source copied once to a writable heap segment and the
     * decode retried. Writable sources and the optimized path never copy.
     */
    static int decompressReadingFrom(MemorySegment src, MemorySegment output, SegmentDecompressor decompressor) {
        try {
            return decompressor.decompress(src, output);
        } catch (IllegalArgumentException refusal) {
            if (!src.isReadOnly()) {
                throw refusal;
            }
            return decompressor.decompress(MemorySegment.ofArray(src.toArray(JAVA_BYTE)), output);
        }
    }

    /** An aircompressor decoder's segment-to-segment decompress method, returning the number of bytes written. */
    @FunctionalInterface
    interface SegmentDecompressor {
        int decompress(MemorySegment src, MemorySegment output);
    }

    /**
     * Runs an aircompressor segment-to-segment encode, tolerating a read-only source under the same rules as
     * {@link #decompressReadingFrom}: the pure-Java compressor reaches the source's base address through {@code Unsafe}
     * and refuses a read-only segment, and platforms without a native backend (the Windows runner) always run it. Only
     * on that refusal, and only for a read-only source, is the source copied once to a writable heap segment and the
     * encode retried; a refusal for any other reason, such as an output buffer too small for the encoded bytes,
     * propagates to the caller.
     */
    static int compressReadingFrom(MemorySegment src, MemorySegment output, SegmentCompressor compressor) {
        try {
            return compressor.compress(src, output);
        } catch (IllegalArgumentException refusal) {
            if (!src.isReadOnly()) {
                throw refusal;
            }
            return compressor.compress(MemorySegment.ofArray(src.toArray(JAVA_BYTE)), output);
        }
    }

    /** An aircompressor encoder's segment-to-segment compress method, returning the number of bytes written. */
    @FunctionalInterface
    interface SegmentCompressor {
        int compress(MemorySegment src, MemorySegment output);
    }

    static void writeFromSegment(MemorySegment src, OutputStream sink) throws IOException {
        byte[] buf = new byte[8192];
        long remaining = src.byteSize();
        long offset = 0L;
        while (remaining > 0L) {
            int chunk = (int) Math.min(buf.length, remaining);
            MemorySegment.copy(src, JAVA_BYTE, offset, buf, 0, chunk);
            sink.write(buf, 0, chunk);
            offset += chunk;
            remaining -= chunk;
        }
    }

    static int streamToSegment(InputStream in, MemorySegment output) throws IOException {
        byte[] buf = new byte[8192];
        int written = 0;
        while (true) {
            int n = in.read(buf);
            if (n < 0) {
                break;
            }
            if ((long) written + n > output.byteSize()) {
                throw new IOException("Output buffer too small (limit=" + output.byteSize() + ")");
            }
            MemorySegment.copy(buf, 0, output, JAVA_BYTE, written, n);
            written += n;
        }
        return written;
    }

    /**
     * {@link OutputStream} adapter that writes into a bounded {@link MemorySegment}, throwing {@link IOException} if
     * the consumer would exceed the segment's byte size.
     */
    static final class BoundedSegmentOutputStream extends OutputStream {

        private final MemorySegment target;
        private final long limit;
        private int written;

        BoundedSegmentOutputStream(MemorySegment target) {
            this.target = target;
            this.limit = target.byteSize();
        }

        @Override
        public void write(int b) throws IOException {
            ensureCapacity(1);
            target.set(JAVA_BYTE, written, (byte) b);
            written++;
        }

        @Override
        public void write(byte[] data, int offset, int length) throws IOException {
            ensureCapacity(length);
            MemorySegment.copy(data, offset, target, JAVA_BYTE, written, length);
            written += length;
        }

        private void ensureCapacity(int additional) throws IOException {
            if ((long) written + additional > limit) {
                throw new IOException("Output buffer too small (limit=" + limit + ")");
            }
        }

        int written() {
            return written;
        }
    }
}
