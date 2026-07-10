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

import java.lang.foreign.MemorySegment;

import io.tileverse.parquetry.format.ParquetLayouts;

/**
 * DELTA_BINARY_PACKED decoder for INT64 columns.
 *
 * <p>Delegates all decoding to {@link DeltaBinaryPackedDecoder} and returns the {@code long} result directly on each
 * call to {@link #next()}.
 */
public final class DeltaBinaryPackedInt64Decoder implements PageDecoder<Long> {

    private final DeltaBinaryPackedDecoder delegate = new DeltaBinaryPackedDecoder();

    @Override
    public void load(MemorySegment page, int valueCount) {
        delegate.load(page);
    }

    @Override
    public Long next() {
        return delegate.next();
    }

    @Override
    public void decodeLongs(int n, long[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = delegate.next();
        }
    }

    @Override
    public void decodeLongs(int n, MemorySegment dst, long dstIndex) {
        for (int i = 0; i < n; i++) {
            dst.setAtIndex(ParquetLayouts.INT64, dstIndex + i, delegate.next());
        }
    }

    @Override
    public void skip(int n) {
        delegate.skip(n);
    }
}
