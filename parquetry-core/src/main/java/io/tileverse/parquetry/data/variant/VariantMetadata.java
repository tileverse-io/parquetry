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
package io.tileverse.parquetry.data.variant;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.io.Segments;

/**
 * Reads a Parquet Variant metadata buffer: the key dictionary shared by a value's objects. The buffer is a header byte,
 * the dictionary size, an offset table, then the concatenated UTF-8 key bytes. Lookups binary-search when the header
 * marks the dictionary sorted and scan linearly otherwise.
 */
public final class VariantMetadata {

    private static final int SUPPORTED_VERSION = 1;

    private final MemorySegment segment;
    private final boolean sorted;
    private final int offsetSize;
    private final int dictionarySize;
    private final long offsetTableStart;
    private final long keyBytesStart;

    public VariantMetadata(MemorySegment metadata) {
        this.segment = metadata;
        int header = readByte(0);
        int version = header & 0x0F;
        if (version != SUPPORTED_VERSION) {
            throw new ParquetFormatException("Unsupported variant metadata version " + version);
        }
        this.sorted = (header & 0x10) != 0;
        this.offsetSize = ((header >> 6) & 0x03) + 1;
        this.dictionarySize = (int) readUnsigned(1, offsetSize);
        this.offsetTableStart = 1L + offsetSize;
        this.keyBytesStart = offsetTableStart + (long) (dictionarySize + 1) * offsetSize;
    }

    public int dictionarySize() {
        return dictionarySize;
    }

    /** The underlying metadata buffer as a fresh {@code byte[]}. */
    byte[] rawBytes() {
        return segment.toArray(ValueLayout.JAVA_BYTE);
    }

    /**
     * Returns a copy of this dictionary backed by a fresh read-only heap segment, decoupled from the batch's page
     * buffer.
     */
    public VariantMetadata detach() {
        MemorySegment copy = Segments.toHeapReadOnly(segment);
        return new VariantMetadata(copy);
    }

    public String key(int id) {
        if (id < 0 || id >= dictionarySize) {
            throw new IndexOutOfBoundsException("key id " + id + " out of [0," + dictionarySize + ")");
        }
        long start = keyBytesStart + readOffset(id);
        long end = keyBytesStart + readOffset(id + 1);
        byte[] keyBytes = segment.asSlice(start, end - start).toArray(ValueLayout.JAVA_BYTE);
        return new String(keyBytes, StandardCharsets.UTF_8);
    }

    public int idOf(String name) {
        if (sorted) {
            return binarySearch(name);
        }
        return linearScan(name);
    }

    private int binarySearch(String name) {
        int low = 0;
        int high = dictionarySize - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int comparison = key(mid).compareTo(name);
            if (comparison < 0) {
                low = mid + 1;
            } else if (comparison > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -1;
    }

    private int linearScan(String name) {
        for (int id = 0; id < dictionarySize; id++) {
            if (key(id).equals(name)) {
                return id;
            }
        }
        return -1;
    }

    private long readOffset(int index) {
        return readUnsigned(offsetTableStart + (long) index * offsetSize, offsetSize);
    }

    private int readByte(long offset) {
        return segment.get(ValueLayout.JAVA_BYTE, offset) & 0xFF;
    }

    private long readUnsigned(long offset, int width) {
        long value = 0L;
        for (int i = 0; i < width; i++) {
            value |= (long) readByte(offset + i) << (8 * i);
        }
        return value;
    }
}
