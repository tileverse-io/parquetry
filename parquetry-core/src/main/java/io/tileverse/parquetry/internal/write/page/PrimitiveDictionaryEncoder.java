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

import java.io.IOException;
import java.util.Arrays;

import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Dictionary attempt for the numeric kinds (INT32, INT64, FLOAT, DOUBLE), keyed by the value's 64-bit pattern instead
 * of boxed objects. Equality follows the boxed wrappers' contract: floats and doubles key on their canonical bits
 * ({@link Float#floatToIntBits}/{@link Double#doubleToLongBits} - every NaN pattern is one dictionary entry, negative
 * and positive zero are distinct), while the retained value keeps the FIRST occurrence's raw bits, which is what the
 * dictionary page emits.
 *
 * <p>Page contract and fallback semantics match the binary counterpart {@link BinaryDictionaryEncoder}: values append
 * to the current page, {@link #flushPage(LittleEndianSink)} closes it; pages encode as {@link Encoding#RLE_DICTIONARY}
 * indices while the dictionary's serialized payload stays within the byte budget, and fall back to
 * {@link Encoding#PLAIN} once it overflows.
 */
public final class PrimitiveDictionaryEncoder implements PageDictionaryEncoder {

    private static final int INITIAL_TABLE_SLOTS = 1 << 10;
    private static final int EMPTY_SLOT = -1;

    private final PrimitiveKind kind;
    private final int valueByteSize;
    private final long dictionaryByteLimit;

    private long[] dictionaryBits = new long[256];
    private int dictionarySize;
    private long[] tableKeys = new long[INITIAL_TABLE_SLOTS];
    private int[] tableIndices = new int[INITIAL_TABLE_SLOTS];

    private long dictionaryBytes;
    private boolean overflowed;
    private boolean emittedDictionaryPage;

    private int[] pageIndices = new int[1024];
    private int pageIndexCount;
    private long[] fallbackBits = new long[1024];
    private int fallbackCount;

    public PrimitiveDictionaryEncoder(PrimitiveKind kind, long dictionaryByteLimit) {
        this.valueByteSize = switch (kind) {
            case INT32, FLOAT -> Integer.BYTES;
            case INT64, DOUBLE -> Long.BYTES;
            default -> throw new ParquetWriteException("Numeric dictionary encoder cannot serve kind " + kind);
        };
        this.kind = kind;
        this.dictionaryByteLimit = dictionaryByteLimit;
        Arrays.fill(tableIndices, EMPTY_SLOT);
    }

    /** Append one INT32 cell to the current page. */
    public void appendInt(int value) {
        append(value, value);
    }

    /** Append one INT64 cell to the current page. */
    public void appendLong(long value) {
        append(value, value);
    }

    /** Append one FLOAT cell; keys on canonical bits, retains the first occurrence's raw bits. */
    public void appendFloat(float value) {
        append(Float.floatToRawIntBits(value), Float.floatToIntBits(value));
    }

    /** Append one DOUBLE cell; keys on canonical bits, retains the first occurrence's raw bits. */
    public void appendDouble(double value) {
        append(Double.doubleToRawLongBits(value), Double.doubleToLongBits(value));
    }

    private void append(long rawBits, long keyBits) {
        if (overflowed) {
            addFallback(rawBits);
            return;
        }
        int slot = findSlot(keyBits);
        int existing = tableIndices[slot];
        if (existing != EMPTY_SLOT) {
            addPageIndex(existing);
            return;
        }
        long candidateBytes = dictionaryBytes + valueByteSize;
        if (candidateBytes > dictionaryByteLimit) {
            overflowToPlain(rawBits);
            return;
        }
        insert(slot, keyBits, rawBits, candidateBytes);
    }

    @Override
    public PageResult flushPage(LittleEndianSink dst) throws IOException {
        if (overflowed) {
            return flushPlainFallbackPage(dst);
        }
        return flushDictionaryPage(dst);
    }

    @Override
    public boolean overflowed() {
        return overflowed;
    }

    @Override
    public boolean emittedDictionaryPage() {
        return emittedDictionaryPage;
    }

    /** The INT32 dictionary values in insertion order. */
    public int[] intCarrier() {
        return intsFrom(dictionaryBits, dictionarySize);
    }

    /** The INT64 dictionary values in insertion order. */
    public long[] longCarrier() {
        return Arrays.copyOf(dictionaryBits, dictionarySize);
    }

    /** The FLOAT dictionary values in insertion order, raw first-occurrence bit patterns. */
    public float[] floatCarrier() {
        return floatsFrom(dictionaryBits, dictionarySize);
    }

    /** The DOUBLE dictionary values in insertion order, raw first-occurrence bit patterns. */
    public double[] doubleCarrier() {
        return doublesFrom(dictionaryBits, dictionarySize);
    }

    private int findSlot(long keyBits) {
        int mask = tableKeys.length - 1;
        int slot = spread(keyBits) & mask;
        while (tableIndices[slot] != EMPTY_SLOT && tableKeys[slot] != keyBits) {
            slot = (slot + 1) & mask;
        }
        return slot;
    }

    /**
     * Finalizing 64-bit mix (Murmur3 fmix64). The raw keys are value bit patterns - real-world floats and sequential
     * integers cluster badly under an identity hash, and linear probing amplifies clustering into long occupied runs. A
     * full avalanche keeps probe chains short regardless of key distribution.
     */
    private static int spread(long keyBits) {
        long h = keyBits;
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return (int) h;
    }

    private void insert(int slot, long keyBits, long rawBits, long newDictionaryBytes) {
        if (dictionarySize == dictionaryBits.length) {
            dictionaryBits = Arrays.copyOf(dictionaryBits, dictionaryBits.length * 2);
        }
        int newIndex = dictionarySize;
        dictionaryBits[newIndex] = rawBits;
        dictionarySize++;
        tableKeys[slot] = keyBits;
        tableIndices[slot] = newIndex;
        dictionaryBytes = newDictionaryBytes;
        addPageIndex(newIndex);
        maybeGrowTable();
    }

    private void maybeGrowTable() {
        // Grow at 2/3 load to keep linear probing short.
        if (dictionarySize * 3L < tableKeys.length * 2L) {
            return;
        }
        long[] oldKeys = tableKeys;
        int[] oldIndices = tableIndices;
        tableKeys = new long[oldKeys.length * 2];
        tableIndices = new int[oldIndices.length * 2];
        Arrays.fill(tableIndices, EMPTY_SLOT);
        int mask = tableKeys.length - 1;
        for (int i = 0; i < oldIndices.length; i++) {
            if (oldIndices[i] == EMPTY_SLOT) {
                continue;
            }
            int slot = spread(oldKeys[i]) & mask;
            while (tableIndices[slot] != EMPTY_SLOT) {
                slot = (slot + 1) & mask;
            }
            tableKeys[slot] = oldKeys[i];
            tableIndices[slot] = oldIndices[i];
        }
    }

    private void overflowToPlain(long rawBits) {
        overflowed = true;
        // Replay the page's indices as raw values: the fallback page must reflect the same row sequence.
        for (int i = 0; i < pageIndexCount; i++) {
            addFallback(dictionaryBits[pageIndices[i]]);
        }
        pageIndexCount = 0;
        addFallback(rawBits);
    }

    private void addPageIndex(int index) {
        if (pageIndexCount == pageIndices.length) {
            pageIndices = Arrays.copyOf(pageIndices, pageIndices.length * 2);
        }
        pageIndices[pageIndexCount++] = index;
    }

    private void addFallback(long rawBits) {
        if (fallbackCount == fallbackBits.length) {
            fallbackBits = Arrays.copyOf(fallbackBits, fallbackBits.length * 2);
        }
        fallbackBits[fallbackCount++] = rawBits;
    }

    private PageResult flushDictionaryPage(LittleEndianSink dst) throws IOException {
        emittedDictionaryPage = true;
        int n = pageIndexCount;
        pageIndexCount = 0;
        RleDictionaryEncoder encoder = new RleDictionaryEncoder();
        int bytesWritten = encoder.encode(pageIndices, n, dst);
        return new PageResult(Encoding.RLE_DICTIONARY, Encoding.PLAIN_DICTIONARY, n, bytesWritten);
    }

    private PageResult flushPlainFallbackPage(LittleEndianSink dst) throws IOException {
        int n = fallbackCount;
        fallbackCount = 0;
        int bytesWritten =
                switch (kind) {
                    case INT32 -> new PlainInt32Encoder().encode(intsFrom(fallbackBits, n), n, dst);
                    case INT64 -> new PlainInt64Encoder().encode(Arrays.copyOf(fallbackBits, n), n, dst);
                    case FLOAT -> new PlainFloatEncoder().encode(floatsFrom(fallbackBits, n), n, dst);
                    case DOUBLE -> new PlainDoubleEncoder().encode(doublesFrom(fallbackBits, n), n, dst);
                    default -> throw new ParquetWriteException("Numeric dictionary encoder cannot serve kind " + kind);
                };
        return new PageResult(Encoding.PLAIN, Encoding.PLAIN, n, bytesWritten);
    }

    private static int[] intsFrom(long[] bits, int n) {
        int[] out = new int[n];
        for (int i = 0; i < n; i++) {
            out[i] = (int) bits[i];
        }
        return out;
    }

    private static float[] floatsFrom(long[] bits, int n) {
        float[] out = new float[n];
        for (int i = 0; i < n; i++) {
            out[i] = Float.intBitsToFloat((int) bits[i]);
        }
        return out;
    }

    private static double[] doublesFrom(long[] bits, int n) {
        double[] out = new double[n];
        for (int i = 0; i < n; i++) {
            out[i] = Double.longBitsToDouble(bits[i]);
        }
        return out;
    }
}
