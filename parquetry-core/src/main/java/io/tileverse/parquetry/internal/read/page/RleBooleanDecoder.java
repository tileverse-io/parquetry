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

import static io.tileverse.parquetry.format.ParquetLayouts.INT32;

import java.lang.foreign.MemorySegment;

/**
 * Boolean data-page decoder using Parquet's RLE encoding (RLE-Bit-Packed hybrid at bitWidth=1).
 *
 * <p>Per the Parquet spec, RLE-encoded data pages are prefixed by a 4-byte little-endian length giving the size of the
 * RLE payload that follows. We strip the prefix in {@link #load} and delegate to a {@link LevelDecoder} for the actual
 * encoded run handling.
 */
public final class RleBooleanDecoder implements PageDecoder<Boolean> {

    private final LevelDecoder delegate = new LevelDecoder(1);

    @Override
    public void load(MemorySegment page, int valueCount) {
        int length = page.get(INT32, 0L);
        MemorySegment payload = page.asSlice(Integer.BYTES, length);
        delegate.load(payload);
    }

    @Override
    public Boolean next() {
        return delegate.nextValue() != 0;
    }

    @Override
    public void decodeBooleans(int n, boolean[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = next();
        }
    }

    @Override
    public void skip(int n) {
        delegate.skip(n);
    }
}
