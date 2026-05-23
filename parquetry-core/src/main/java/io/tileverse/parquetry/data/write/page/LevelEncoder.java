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
package io.tileverse.parquetry.data.write.page;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import io.tileverse.parquetry.data.read.page.LevelDecoder;

/**
 * Encodes Parquet repetition and definition level streams. Inverse of {@link LevelDecoder}.
 *
 * <p>The encoded byte sequence uses the RLE-Bit-Packed hybrid encoding at a bit width derived from {@code maxLevel}. A
 * bit width of zero short-circuits to an empty output, matching the decoder's "bitWidth==0 returns 0 always" behavior.
 */
public final class LevelEncoder {

    private final int bitWidth;

    /** @param maxLevel max possible level value; bit width is {@code ceil(log2(maxLevel+1))} */
    public LevelEncoder(int maxLevel) {
        if (maxLevel < 0) {
            throw new IllegalArgumentException("maxLevel must be non-negative; got " + maxLevel);
        }
        this.bitWidth = LevelDecoder.computeBitWidth(maxLevel);
    }

    /** The encoder's bit width, computed from {@code maxLevel} via {@link LevelDecoder#computeBitWidth(int)}. */
    public int bitWidth() {
        return bitWidth;
    }

    /** Encode the first {@code n} level values from {@code levels} into {@code dst}. Returns bytes written. */
    public int encode(int[] levels, int n, WritableByteChannel dst) throws IOException {
        return RleBitPackedHybridWriter.write(levels, n, bitWidth, dst);
    }
}
