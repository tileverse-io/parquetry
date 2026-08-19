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
 * DELTA_BINARY_PACKED encoder for INT64 columns.
 *
 * <p>Delegates to {@link DeltaBinaryPackedWriter}. Inverse of
 * {@link io.tileverse.parquetry.internal.read.page.DeltaBinaryPackedInt64Decoder}.
 */
public final class DeltaBinaryPackedInt64Encoder implements Encoder<long[]> {

    @Override
    public int encode(long[] values, int n, LittleEndianSink dst) throws IOException {
        return DeltaBinaryPackedWriter.write(values, n, dst);
    }

    @Override
    public Encoding parquetEncoding() {
        return Encoding.DELTA_BINARY_PACKED;
    }
}
