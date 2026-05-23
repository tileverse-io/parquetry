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

import io.tileverse.parquetry.format.Encoding;

/**
 * DELTA_BINARY_PACKED encoder for INT32 columns.
 *
 * <p>Widens the input to {@code long} and delegates to {@link DeltaBinaryPackedWriter}. Inverse of
 * {@link io.tileverse.parquetry.data.read.page.DeltaBinaryPackedInt32Decoder}.
 */
public final class DeltaBinaryPackedInt32Encoder implements Encoder<int[]> {

    @Override
    public int encode(int[] values, int n, WritableByteChannel dst) throws IOException {
        long[] widened = new long[n];
        for (int i = 0; i < n; i++) {
            widened[i] = values[i];
        }
        return DeltaBinaryPackedWriter.write(widened, n, dst);
    }

    @Override
    public Encoding parquetEncoding() {
        return Encoding.DELTA_BINARY_PACKED;
    }
}
