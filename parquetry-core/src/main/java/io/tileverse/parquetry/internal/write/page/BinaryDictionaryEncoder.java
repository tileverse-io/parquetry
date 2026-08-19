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
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.tileverse.parquetry.format.Encoding;

import io.airlift.compress.v3.xxhash.XxHash64Hasher;

/**
 * Dictionary attempt for the binary kinds (BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, packed INT96), keyed by content hash
 * instead of boxed values. Values arrive as {@link MemorySegment} views of caller-owned memory: a lookup hashes the
 * segment (xxHash64) into an open-addressed table and confirms a hit with a byte compare; only a distinct value's bytes
 * are copied out of the segment and retained - a repeated value allocates nothing.
 *
 * <p>Page contract and fallback semantics match the {@link PageDictionaryEncoder} contract: values append to the
 * current page, {@link #flushPage(LittleEndianSink)} closes it; pages encode as {@link Encoding#RLE_DICTIONARY} indices
 * while the dictionary's serialized payload stays within the byte budget, and fall back to {@link Encoding#PLAIN} once
 * it overflows.
 */
public final class BinaryDictionaryEncoder implements PageDictionaryEncoder {

    private static final int INITIAL_TABLE_SLOTS = 1 << 10;
    private static final int EMPTY_SLOT = -1;

    private final Encoder<BinaryPayload> plainEncoder;

    /** Fixed serialized size per value, or -1 for variable length (a 4-byte length prefix plus the bytes). */
    private final int fixedValueLength;

    private final long dictionaryByteLimit;

    private final List<byte[]> dictionaryValues = new ArrayList<>();
    private final List<MemorySegment> dictionarySegments = new ArrayList<>();
    private long[] tableHashes;
    private int[] tableIndices;

    private long dictionaryBytes;
    private boolean overflowed;
    private boolean emittedDictionaryPage;

    private int[] pageIndices = new int[1024];
    private int pageIndexCount;
    private final List<byte[]> pageFallbackValues = new ArrayList<>();

    private BinaryDictionaryEncoder(
            Encoder<BinaryPayload> plainEncoder, int fixedValueLength, long dictionaryByteLimit) {
        this.plainEncoder = plainEncoder;
        this.fixedValueLength = fixedValueLength;
        this.dictionaryByteLimit = dictionaryByteLimit;
        this.tableHashes = new long[INITIAL_TABLE_SLOTS];
        this.tableIndices = new int[INITIAL_TABLE_SLOTS];
        Arrays.fill(tableIndices, EMPTY_SLOT);
    }

    /** Encoder for BYTE_ARRAY columns; each dictionary value costs a 4-byte length prefix plus its bytes. */
    public static BinaryDictionaryEncoder variableLength(
            Encoder<BinaryPayload> plainEncoder, long dictionaryByteLimit) {
        return new BinaryDictionaryEncoder(plainEncoder, -1, dictionaryByteLimit);
    }

    /** Encoder for FIXED_LEN_BYTE_ARRAY / INT96 columns; each dictionary value costs exactly {@code valueLength}. */
    public static BinaryDictionaryEncoder fixedLength(
            Encoder<BinaryPayload> plainEncoder, int valueLength, long dictionaryByteLimit) {
        return new BinaryDictionaryEncoder(plainEncoder, valueLength, dictionaryByteLimit);
    }

    /**
     * Append one value to the current page. The segment is hashed and compared against the retained distinct values; a
     * distinct value's bytes are copied when it joins the dictionary or the plain fallback, and a repeat only records
     * its index. The segment is not touched after this call returns.
     */
    public void appendValue(MemorySegment value) {
        append(value, null);
    }

    /**
     * Append one value the caller already holds as a packed array (the INT96 path); the array itself is what the
     * dictionary keeps when the value turns out to be distinct.
     */
    public void appendValue(byte[] retainable) {
        append(MemorySegment.ofArray(retainable), retainable);
    }

    /** {@code retainable} is the array to keep on insert or overflow, or null to copy the segment at that point. */
    private void append(MemorySegment value, byte[] retainable) {
        if (overflowed) {
            pageFallbackValues.add(retain(value, retainable));
            return;
        }
        long hash = XxHash64Hasher.hash(value);
        int slot = findSlot(hash, value);
        int existing = tableIndices[slot];
        if (existing != EMPTY_SLOT) {
            addPageIndex(existing);
            return;
        }
        long candidateBytes = dictionaryBytes + sizeOf(value.byteSize());
        if (candidateBytes > dictionaryByteLimit) {
            overflowToPlain(retain(value, retainable));
            return;
        }
        insert(slot, hash, retain(value, retainable), candidateBytes);
    }

    private static byte[] retain(MemorySegment value, byte[] retainable) {
        return retainable != null ? retainable : value.toArray(ValueLayout.JAVA_BYTE);
    }

    @Override
    public PageResult flushPage(LittleEndianSink dst) throws IOException {
        if (overflowed) {
            return flushPlainFallbackPage(dst);
        }
        return flushDictionaryPage(dst);
    }

    /** The chunk dictionary's distinct values in insertion order, as the dictionary page's carrier. */
    public byte[][] dictionaryCarrier() {
        return dictionaryValues.toArray(new byte[0][]);
    }

    @Override
    public boolean overflowed() {
        return overflowed;
    }

    @Override
    public boolean emittedDictionaryPage() {
        return emittedDictionaryPage;
    }

    /** Returns the slot holding this value's index, or the empty slot where it would insert. */
    private int findSlot(long hash, MemorySegment value) {
        int mask = tableHashes.length - 1;
        int slot = Long.hashCode(hash) & mask;
        while (true) {
            int index = tableIndices[slot];
            if (index == EMPTY_SLOT) {
                return slot;
            }
            if (tableHashes[slot] == hash && value.mismatch(dictionarySegments.get(index)) == -1) {
                return slot;
            }
            slot = (slot + 1) & mask;
        }
    }

    private void insert(int slot, long hash, byte[] retainable, long newDictionaryBytes) {
        int newIndex = dictionaryValues.size();
        dictionaryValues.add(retainable);
        dictionarySegments.add(MemorySegment.ofArray(retainable).asReadOnly());
        tableHashes[slot] = hash;
        tableIndices[slot] = newIndex;
        dictionaryBytes = newDictionaryBytes;
        addPageIndex(newIndex);
        maybeGrowTable();
    }

    private void maybeGrowTable() {
        // Grow at 2/3 load to keep linear probing short.
        if (dictionaryValues.size() * 3L < tableHashes.length * 2L) {
            return;
        }
        long[] oldHashes = tableHashes;
        int[] oldIndices = tableIndices;
        tableHashes = new long[oldHashes.length * 2];
        tableIndices = new int[oldIndices.length * 2];
        Arrays.fill(tableIndices, EMPTY_SLOT);
        int mask = tableHashes.length - 1;
        for (int i = 0; i < oldIndices.length; i++) {
            if (oldIndices[i] == EMPTY_SLOT) {
                continue;
            }
            int slot = Long.hashCode(oldHashes[i]) & mask;
            while (tableIndices[slot] != EMPTY_SLOT) {
                slot = (slot + 1) & mask;
            }
            tableHashes[slot] = oldHashes[i];
            tableIndices[slot] = oldIndices[i];
        }
    }

    private long sizeOf(long valueLength) {
        if (fixedValueLength >= 0) {
            return fixedValueLength;
        }
        return Integer.BYTES + valueLength;
    }

    private void overflowToPlain(byte[] value) {
        overflowed = true;
        // Replay the page's indices as raw values: the fallback page must reflect the same row sequence.
        for (int i = 0; i < pageIndexCount; i++) {
            pageFallbackValues.add(dictionaryValues.get(pageIndices[i]));
        }
        pageIndexCount = 0;
        pageFallbackValues.add(value);
    }

    private void addPageIndex(int index) {
        if (pageIndexCount == pageIndices.length) {
            pageIndices = Arrays.copyOf(pageIndices, pageIndices.length * 2);
        }
        pageIndices[pageIndexCount++] = index;
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
        int n = pageFallbackValues.size();
        byte[][] carrier = pageFallbackValues.toArray(new byte[0][]);
        pageFallbackValues.clear();
        int bytesWritten = plainEncoder.encode(new ArrayBinaryPayload(carrier, n), n, dst);
        return new PageResult(Encoding.PLAIN, Encoding.PLAIN, n, bytesWritten);
    }
}
