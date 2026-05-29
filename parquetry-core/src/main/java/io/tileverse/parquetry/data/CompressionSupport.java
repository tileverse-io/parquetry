/*
 * Copyright (c) 2026 Multivers.io
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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;

/** Package-private helpers shared by {@link Compression} record implementations. */
final class CompressionSupport {

    private CompressionSupport() {}

    static int intExact(long uncompressedLength) {
        if (uncompressedLength < 0 || uncompressedLength > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("uncompressedLength out of range: " + uncompressedLength);
        }
        return (int) uncompressedLength;
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
                throw new IOException("GZIP output buffer too small (limit=" + limit + ")");
            }
        }

        int written() {
            return written;
        }
    }
}
