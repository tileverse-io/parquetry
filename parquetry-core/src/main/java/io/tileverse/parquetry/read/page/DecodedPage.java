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
package io.tileverse.parquetry.read.page;

import java.nio.ByteBuffer;
import java.util.Objects;

import io.tileverse.parquetry.format.enums.Encoding;

import io.tileverse.io.ByteBufferPool.PooledByteBuffer;

/**
 * The uniform {@code (repLevels, defLevels, values)} triple produced by a {@link DataPageReader} for one data page.
 *
 * <p>Buffer-ownership asymmetry (intentional, see the package documentation):
 *
 * <ul>
 *   <li>{@link #repLevelBytes()} and {@link #defLevelBytes()} are <strong>zero-copy read-only slices</strong> of the
 *       parent compressed column-chunk buffer. They are <em>not</em> pooled and must not be closed independently; their
 *       lifetime is bound to the parent {@code FetchedColumnChunk}'s pooled buffer (which {@code RowGroupReader}
 *       closes). Either buffer is an empty read-only buffer when the column's corresponding max level is zero.
 *   <li>{@link #valueBuffer()} is a {@link PooledByteBuffer} holding the <em>decompressed</em> value bytes for this
 *       page. Closing the {@code DecodedPage} returns that buffer to the pool. The column reader closes the previous
 *       page's {@code DecodedPage} before borrowing the next, so steady-state allocation is one decompressed-page-sized
 *       pooled buffer per column.
 * </ul>
 *
 * <p>{@link #valuesEncoding()} is already normalized: {@link Encoding#PLAIN_DICTIONARY} from Parquet 1.x writers is
 * collapsed to {@link Encoding#RLE_DICTIONARY} (see {@link EncodingNormalizer}).
 *
 * @param valueCount values yielded by this page (matches the page header's {@code numValues})
 * @param valuesEncoding the value encoding to drive {@code DecoderFactory.decoderFor(...)}; already normalized
 * @param repLevelBytes read-only zero-copy slice of the rep-level bytes; empty when the column has {@code maxRep == 0}
 * @param defLevelBytes read-only zero-copy slice of the def-level bytes; empty when the column has {@code maxDef == 0}
 * @param valueBuffer pooled buffer holding the decompressed value bytes; closing this {@code DecodedPage} returns it to
 *     the pool
 */
public record DecodedPage(
        int valueCount,
        Encoding valuesEncoding,
        ByteBuffer repLevelBytes,
        ByteBuffer defLevelBytes,
        PooledByteBuffer valueBuffer)
        implements AutoCloseable {

    public DecodedPage {
        Objects.requireNonNull(valuesEncoding, "valuesEncoding");
        Objects.requireNonNull(repLevelBytes, "repLevelBytes");
        Objects.requireNonNull(defLevelBytes, "defLevelBytes");
        Objects.requireNonNull(valueBuffer, "valueBuffer");
        if (valueCount < 0) {
            throw new IllegalArgumentException("valueCount must be >= 0, got " + valueCount);
        }
    }

    /**
     * Returns the decompressed value bytes, ready to read. Equivalent to {@code valueBuffer().buffer()}; provided as a
     * convenience for decoders that expect a {@link ByteBuffer} directly.
     */
    public ByteBuffer values() {
        return valueBuffer.buffer();
    }

    /**
     * Returns the pooled value buffer to the pool. Closing is idempotent (the underlying {@code PooledByteBuffer.close}
     * is). The level slices are <strong>not</strong> released here - they belong to the parent column-chunk buffer.
     */
    @Override
    public void close() {
        valueBuffer.close();
    }
}
