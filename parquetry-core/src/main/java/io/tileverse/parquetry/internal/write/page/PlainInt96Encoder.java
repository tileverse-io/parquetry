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
 * PLAIN encoder for INT96: contiguous 12-byte values.
 *
 * <p>Inverse of {@link io.tileverse.parquetry.internal.read.page.PlainInt96Decoder}. INT96 is a deprecated legacy
 * timestamp type; each value is written as 12 raw bytes (whose internal layout is up to the caller; the decoder also
 * yields raw bytes).
 */
public final class PlainInt96Encoder implements Encoder<BinaryPayload> {

    private static final int INT96_BYTES = 12;

    @Override
    public int encode(BinaryPayload values, int n, LittleEndianSink dst) throws IOException {
        for (int i = 0; i < n; i++) {
            int valueLength = values.length(i);
            if (valueLength != INT96_BYTES) {
                throw new IllegalArgumentException(
                        "INT96 value at index " + i + " has length " + valueLength + " but must be " + INT96_BYTES);
            }
            values.writeValueInto(i, dst);
        }
        return n * INT96_BYTES;
    }

    @Override
    public Encoding parquetEncoding() {
        return Encoding.PLAIN;
    }
}
