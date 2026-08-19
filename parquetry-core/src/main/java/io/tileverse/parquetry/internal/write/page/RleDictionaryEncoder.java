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
 * Dictionary index encoder. Emits the bit-width byte followed by an RLE/bit-pack hybrid stream of dictionary indices.
 *
 * <p>Inverse of {@link io.tileverse.parquetry.internal.read.page.RleDictionaryPageDecoder}. The carrier {@code int[]}
 * contains the indices into a column-chunk dictionary (not the dictionary values themselves; those are written
 * separately as a PLAIN-encoded dictionary page).
 *
 * <p>Returns {@link Encoding#RLE_DICTIONARY} for the DataPage V2 page-header marker; {@link Encoding#PLAIN_DICTIONARY}
 * for V1.
 */
public final class RleDictionaryEncoder implements Encoder<int[]> {

    @Override
    public int encode(int[] values, int n, LittleEndianSink dst) throws IOException {
        int maxIndex = 0;
        for (int i = 0; i < n; i++) {
            if (values[i] > maxIndex) {
                maxIndex = values[i];
            }
        }
        int bitWidth = (maxIndex == 0) ? 0 : 32 - Integer.numberOfLeadingZeros(maxIndex);
        dst.writeByte(bitWidth);
        int payloadBytes = (bitWidth == 0)
                ? RleBitPackedHybridWriter.writeZeroWidthRun(n, dst)
                : RleBitPackedHybridWriter.write(values, n, bitWidth, dst);
        return 1 + payloadBytes;
    }

    @Override
    public Encoding parquetEncoding() {
        return Encoding.RLE_DICTIONARY;
    }

    @Override
    public Encoding parquetEncodingV1() {
        return Encoding.PLAIN_DICTIONARY;
    }
}
