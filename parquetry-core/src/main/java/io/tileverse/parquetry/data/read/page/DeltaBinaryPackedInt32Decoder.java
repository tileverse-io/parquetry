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
package io.tileverse.parquetry.data.read.page;

import java.nio.ByteBuffer;

/**
 * DELTA_BINARY_PACKED decoder for INT32 columns.
 *
 * <p>Delegates all decoding to {@link DeltaBinaryPackedDecoder} and narrows the {@code long} result to {@code int} on
 * each call to {@link #next()}.
 */
public final class DeltaBinaryPackedInt32Decoder implements PageDecoder<Integer> {

    private final DeltaBinaryPackedDecoder delegate = new DeltaBinaryPackedDecoder();

    @Override
    public void load(ByteBuffer page, int valueCount) {
        delegate.load(page);
    }

    @Override
    public Integer next() {
        return (int) delegate.next();
    }

    @Override
    public void decodeInts(int n, int[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = (int) delegate.next();
        }
    }

    @Override
    public void skip(int n) {
        delegate.skip(n);
    }
}
