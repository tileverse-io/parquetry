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
package io.tileverse.parquetry.internal.read.page;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;

import io.tileverse.parquetry.format.MalformedFileException;

/**
 * PLAIN decoder for BOOLEAN: bit-packed, LSB-first, eight values per byte.
 *
 * <p>Bit 0 of byte 0 is value 0, bit 1 of byte 0 is value 1, etc. The decoder maintains an internal bit cursor and
 * reads each bit straight from the page segment, so it neither copies the page nor allocates.
 */
public final class PlainBooleanDecoder implements PageDecoder<Boolean> {

    private MemorySegment segment;
    private int storedBitCount;
    private int bitPosition;

    @Override
    public void load(MemorySegment page, int valueCount) {
        long storedBits = page.byteSize() * Byte.SIZE;
        if (storedBits < valueCount) {
            throw new MalformedFileException(
                    valueCount + " PLAIN BOOLEAN values need " + valueCount + " bits but the page holds " + storedBits);
        }
        this.segment = page;
        // The int bit cursor stops at Integer.MAX_VALUE: no bit past it is ever read.
        this.storedBitCount = (int) Math.min(storedBits, Integer.MAX_VALUE);
        this.bitPosition = 0;
    }

    @Override
    public Boolean next() {
        if (bitPosition >= storedBitCount) {
            throw exhausted(bitPosition, storedBitCount);
        }
        int index = bitPosition++;
        int b = segment.get(JAVA_BYTE, index >> 3) & 0xff;
        return ((b >> (index & 7)) & 1) != 0;
    }

    /** Built out of line: {@link #next()} runs once per value, and the JIT only inlines it while it stays small. */
    private static IllegalStateException exhausted(int bitPosition, int storedBitCount) {
        return new IllegalStateException(
                "PlainBooleanDecoder exhausted: position " + bitPosition + " >= storedBitCount " + storedBitCount);
    }

    @Override
    public void decodeBooleans(int n, boolean[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = next();
        }
    }

    @Override
    public void skip(int n) {
        bitPosition += n;
    }
}
