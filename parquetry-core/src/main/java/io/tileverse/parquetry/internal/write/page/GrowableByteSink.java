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
package io.tileverse.parquetry.internal.write.page;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

/**
 * Array-backed {@link LittleEndianSink} whose backing array grows on demand. Owned by a single {@link PageWriter} /
 * {@code ColumnChunkWriter} and reused across pages via {@link #reset()}; never shared across threads.
 */
public final class GrowableByteSink implements LittleEndianSink {

    private byte[] buf;
    private int count;

    public GrowableByteSink(int initialCapacity) {
        this.buf = new byte[Math.max(1, initialCapacity)];
        this.count = 0;
    }

    @Override
    public void writeByte(int b) {
        ensureCapacity(1);
        buf[count] = (byte) b;
        count += 1;
    }

    @Override
    public void writeInt(int v) {
        ensureCapacity(Integer.BYTES);
        buf[count] = (byte) v;
        buf[count + 1] = (byte) (v >>> 8);
        buf[count + 2] = (byte) (v >>> 16);
        buf[count + 3] = (byte) (v >>> 24);
        count += Integer.BYTES;
    }

    @Override
    public void writeLong(long v) {
        ensureCapacity(Long.BYTES);
        buf[count] = (byte) v;
        buf[count + 1] = (byte) (v >>> 8);
        buf[count + 2] = (byte) (v >>> 16);
        buf[count + 3] = (byte) (v >>> 24);
        buf[count + 4] = (byte) (v >>> 32);
        buf[count + 5] = (byte) (v >>> 40);
        buf[count + 6] = (byte) (v >>> 48);
        buf[count + 7] = (byte) (v >>> 56);
        count += Long.BYTES;
    }

    @Override
    public void writeFloat(float v) {
        writeInt(Float.floatToRawIntBits(v));
    }

    @Override
    public void writeDouble(double v) {
        writeLong(Double.doubleToRawLongBits(v));
    }

    @Override
    public void write(byte[] src, int off, int len) {
        ensureCapacity(len);
        System.arraycopy(src, off, buf, count, len);
        count += len;
    }

    @Override
    public void write(MemorySegment src, long off, long len) {
        int intLen = Math.toIntExact(len);
        ensureCapacity(intLen);
        MemorySegment.copy(src, ValueLayout.JAVA_BYTE, off, buf, count, intLen);
        count += intLen;
    }

    @Override
    public int size() {
        return count;
    }

    /** Appends this sink's current content to {@code dst}. */
    public void writeInto(LittleEndianSink dst) {
        dst.write(buf, 0, count);
    }

    /** Drops all content, keeping capacity for the next page. */
    public void reset() {
        count = 0;
    }

    /**
     * The backing array; readers must honor {@code [0, size())}. The view aliases the live backing array and is valid
     * only until the next write, {@link #reset()}, or growth; copy via {@link #toByteArray()} to retain it.
     */
    public byte[] array() {
        return buf;
    }

    /** A right-sized copy of {@code [0, size())}. */
    public byte[] toByteArray() {
        return Arrays.copyOf(buf, count);
    }

    /**
     * A read-only heap segment over {@code [0, size())} for a heap-to-heap consumer. The view aliases the live backing
     * array and is valid only until the next write, {@link #reset()}, or growth; copy via {@link #toByteArray()} to
     * retain it.
     */
    public MemorySegment heapSegment() {
        return MemorySegment.ofArray(buf).asSlice(0L, count).asReadOnly();
    }

    private void ensureCapacity(int extra) {
        long needed = (long) count + extra;
        if (needed <= buf.length) {
            return;
        }
        long grown = Math.max((long) buf.length * 2, needed);
        this.buf = Arrays.copyOf(buf, Math.toIntExact(grown));
    }
}
