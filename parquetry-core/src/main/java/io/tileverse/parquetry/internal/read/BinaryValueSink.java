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
package io.tileverse.parquetry.internal.read;

import java.lang.foreign.MemorySegment;
import java.util.BitSet;

/**
 * Accumulates binary values into a single heap backing buffer plus dense row offsets, copying each value's bytes
 * straight from the page with no per-value {@link MemorySegment} object. One instance is reused per column reader; each
 * {@link #reset(int, int)} allocates a fresh, exactly-sized backing and offset array that outlive the page and are
 * handed to the vector.
 */
public final class BinaryValueSink {

    private MemorySegment backing;
    private int[] offsets;
    private int writePos;
    private int count;

    /** Starts a new run of at most {@code maxValues} values totalling exactly {@code totalBytes} bytes. */
    public void reset(int maxValues, int totalBytes) {
        this.backing = MemorySegment.ofArray(new byte[totalBytes]);
        this.offsets = new int[maxValues + 1];
        this.writePos = 0;
        this.count = 0;
    }

    /** Copies {@code len} bytes from {@code src} at {@code srcOffset} as the next value. */
    public void appendValue(MemorySegment src, long srcOffset, int len) {
        offsets[count] = writePos;
        MemorySegment.copy(src, srcOffset, backing, writePos, len);
        writePos += len;
        count++;
    }

    public int count() {
        return count;
    }

    /** The dense offsets, length {@code count + 1}; the closing offset is the total bytes written. */
    public int[] offsets() {
        offsets[count] = writePos;
        if (offsets.length == count + 1) {
            return offsets;
        }
        int[] exact = new int[count + 1];
        System.arraycopy(offsets, 0, exact, 0, count + 1);
        return exact;
    }

    /** Read-only view of the bytes written. */
    public MemorySegment backing() {
        return backing.asSlice(0, writePos).asReadOnly();
    }

    /**
     * Expands dense offsets (one window per present row, in order) to row-indexed offsets over the same backing, each
     * null row a zero-length run. {@code rowOffsets[i]} equals the dense offset of the number of present rows before
     * {@code i}.
     */
    public static int[] spreadOffsets(int[] denseOffsets, BitSet validity, int rowCount) {
        int[] rowOffsets = new int[rowCount + 1];
        int dense = 0;
        for (int row = 0; row < rowCount; row++) {
            rowOffsets[row] = denseOffsets[dense];
            if (validity.get(row)) {
                dense++;
            }
        }
        rowOffsets[rowCount] = denseOffsets[dense];
        return rowOffsets;
    }
}
