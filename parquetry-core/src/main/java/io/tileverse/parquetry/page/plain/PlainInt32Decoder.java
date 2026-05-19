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
import java.nio.ByteOrder;
import java.nio.IntBuffer;

import io.tileverse.parquetry.page.PageDecoder;

/** PLAIN decoder for INT32: four bytes, little-endian per value. */
public final class PlainInt32Decoder implements PageDecoder<Integer> {

    private IntBuffer buffer;

    // PLAIN encoding stores only non-null values; the page header's valueCount is the logical count
    // (nulls included), not the count actually present in the buffer. The buffer's natural capacity
    // already reflects what was written - tighter bounds would reject valid pages where every value
    // is null. Underflow from over-consumption is caught by the buffer at next().
    @Override
    public void load(ByteBuffer page, int valueCount) {
        this.buffer = page.order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
    }

    @Override
    public Integer next() {
        return buffer.get();
    }

    @Override
    public void skip(int n) {
        buffer.position(buffer.position() + n);
    }
}
