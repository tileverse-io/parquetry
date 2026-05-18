/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.page.plain;

import java.nio.ByteBuffer;
import java.util.BitSet;

import io.tileverse.parquetry.page.PageDecoder;

/**
 * PLAIN decoder for BOOLEAN: bit-packed, LSB-first, eight values per byte.
 *
 * <p>Bit 0 of byte 0 is value 0, bit 1 of byte 0 is value 1, etc. The decoder maintains an internal bit cursor and does
 * not advance the underlying buffer's position on each call, keeping it simple and allocation-free.
 */
public final class PlainBooleanDecoder implements PageDecoder<Boolean> {

    private int valueCount;
    private int bitPosition;
    private BitSet bitset;

    @Override
    public void load(ByteBuffer page, int valueCount) {
        this.valueCount = valueCount;
        this.bitPosition = 0;
        this.bitset = BitSet.valueOf(page);
    }

    @Override
    public Boolean next() {
        if (bitPosition == valueCount) {
            throw new IllegalStateException();
        }
        return bitset.get(bitPosition++);
    }

    @Override
    public void skip(int n) {
        bitPosition += n;
    }
}
