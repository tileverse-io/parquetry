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

import io.tileverse.parquetry.codec.Codec;
import io.tileverse.parquetry.format.PageHeader;

import io.tileverse.io.ByteBufferPool;

/**
 * Parquet 1.x Data Page V1 reader. Placeholder; the V1 read path lands as part of the Parquet 1.x compatibility work
 * conformance work. Until then this class exists only to satisfy the {@link DataPageReader} sealed permits clause so A
 * later change can ship the V2 read path behind the same abstraction.
 */
public final class DataPageV1Reader implements DataPageReader {

    // TODO: when the V1 read path is implemented, call EncodingNormalizer.normalize()
    // on the page encoding before constructing the DecodedPage so the "encoding is normalized"
    // contract on DecodedPage.valuesEncoding() holds for V1 files too.
    @Override
    public DecodedPage read(PageHeader header, ByteBuffer compressedPagePayload, Codec codec, ByteBufferPool pool) {
        throw new UnsupportedOperationException(
                "Data Page V1 read lands in a later change; only V2 pages are decoded today");
    }
}
