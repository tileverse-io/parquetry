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

import io.tileverse.parquetry.format.Encoding;

/**
 * RLE encoder for BOOLEAN (DataPage V2 path): RLE-Bit-Packed hybrid stream at bitWidth=1, prefixed by a 4-byte
 * little-endian length giving the size of the RLE payload that follows.
 *
 * <p>Inverse of {@link io.tileverse.parquetry.internal.read.page.RleBooleanDecoder}. The length-prefix convention is
 * the same one Parquet uses for V1 RLE-encoded data pages.
 */
public final class RleBooleanEncoder implements Encoder<boolean[]> {

    @Override
    public int encode(boolean[] values, int n, LittleEndianSink dst) throws IOException {
        int[] asInts = new int[n];
        for (int i = 0; i < n; i++) {
            asInts[i] = values[i] ? 1 : 0;
        }
        GrowableByteSink payload = new GrowableByteSink(64);
        RleBitPackedHybridWriter.write(asInts, n, 1, payload);
        dst.writeInt(payload.size());
        payload.writeInto(dst);
        return Integer.BYTES + payload.size();
    }

    @Override
    public Encoding parquetEncoding() {
        return Encoding.RLE;
    }
}
