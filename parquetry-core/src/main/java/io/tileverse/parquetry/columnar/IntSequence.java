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
package io.tileverse.parquetry.columnar;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Immutable read-only sequence of 32-bit ints backing vector offsets and dictionary indexes. The backing is either a
 * heap {@code int[]} or an off-heap segment of little-endian 32-bit ints; consumers never learn which. Like
 * {@link Validity} and {@link Levels}, the instance owns its representation: {@link #of(int[])} takes ownership of a
 * freshly built array the caller must not mutate afterwards, and no accessor exposes the backing.
 *
 * <p>{@link #ofSegment(MemorySegment, int)} reads i32 elements unaligned in little-endian order, because pooled
 * segments make no alignment promise. The segment factory documents a lifetime, not an ownership transfer: the caller
 * retains the segment and must keep it valid (and unchanged) for the lifetime of this instance.
 *
 * <p>{@link #copyInto(MemorySegment, long)} exports the sequence as little-endian i32 bytes for serialization,
 * identically for both backings.
 */
public final class IntSequence {

    private static final ValueLayout.OfInt I32 = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    private final int[] array; // null == not heap-backed
    private final MemorySegment segment; // null == not segment-backed; little-endian i32, read unaligned
    private final int size;

    private IntSequence(int[] array, MemorySegment segment, int size) {
        this.array = array;
        this.segment = segment;
        this.size = size;
    }

    /** Wraps a freshly built int array, taking ownership of it; the caller must not mutate it afterwards. */
    public static IntSequence of(int[] values) {
        Objects.requireNonNull(values, "values");
        return new IntSequence(values, null, values.length);
    }

    /**
     * A view over {@code size} little-endian 32-bit ints at the start of {@code segment}, read unaligned. The caller
     * retains ownership of the segment for the lifetime of this instance; the segment may be larger than {@code 4 *
     * size} bytes.
     */
    public static IntSequence ofSegment(MemorySegment segment, int size) {
        Objects.requireNonNull(segment, "segment");
        return new IntSequence(null, segment.asReadOnly(), size);
    }

    /** Number of int entries. */
    public int size() {
        return size;
    }

    /**
     * The value at {@code index}. Unlike {@link Levels} and {@link Validity}, this rejects indexes outside {@code [0,
     * size())}, including reads past {@code size} into an oversized segment: a pooled segment's stale trailing bytes
     * must not be readable.
     */
    public int get(int index) {
        Objects.checkIndex(index, size);
        return array != null ? array[index] : segment.getAtIndex(I32, index);
    }

    /** The heap estimate for vectors' {@code approximateHeapBytes}; an off-heap backing is accounted by its owner. */
    public long heapBytes() {
        return array != null ? (long) size * Integer.BYTES : 0L;
    }

    /** Writes the sequence's {@code size * 4} little-endian bytes into {@code dst} at {@code dstOffset}. */
    public void copyInto(MemorySegment dst, long dstOffset) {
        copyInto(0, size, dst, dstOffset);
    }

    /**
     * Writes the {@code count} entries starting at {@code from} as {@code count * 4} little-endian bytes into
     * {@code dst} at {@code dstOffset}. The window {@code [from, from + count)} must lie within {@code [0, size())};
     * like {@link #get(int)}, a window reaching past {@code size} into an oversized segment is rejected.
     */
    public void copyInto(int from, int count, MemorySegment dst, long dstOffset) {
        Objects.checkFromIndexSize(from, count, size);
        if (array != null) {
            MemorySegment.copy(array, from, dst, I32, dstOffset, count);
        } else {
            MemorySegment.copy(segment, (long) from * Integer.BYTES, dst, dstOffset, (long) count * Integer.BYTES);
        }
    }
}
