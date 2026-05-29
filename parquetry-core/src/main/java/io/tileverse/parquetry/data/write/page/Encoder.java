/*
 * Copyright (c) 2026 Multivers.io
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
 * Encodes {@code n} values of a primitive-specialized carrier {@code T} into a single page's value-bytes section.
 *
 * <p>Mirror of {@link io.tileverse.parquetry.data.read.page.PageDecoder} for the write path. Implementations are
 * stateless across pages: a caller can reuse the same {@code Encoder<T>} to write many pages back-to-back by calling
 * {@link #encode(Object, int, WritableByteChannel)} once per page.
 *
 * <p>{@link #parquetEncoding()} returns the marker that goes in the page header's {@code encoding} field for DataPage
 * V2 writers; {@link #parquetEncodingV1()} returns the marker for DataPage V1 writers. The two values only differ for
 * dictionary-encoded data, where V1 uses {@link Encoding#PLAIN_DICTIONARY} and V2 uses {@link Encoding#RLE_DICTIONARY}.
 *
 * @param <T> the carrier array type for this encoder ({@code int[]}, {@code long[]}, {@code byte[][]}, etc.)
 */
public sealed interface Encoder<T>
        permits PlainInt32Encoder,
                PlainInt64Encoder,
                PlainFloatEncoder,
                PlainDoubleEncoder,
                PlainBooleanEncoder,
                PlainBinaryEncoder,
                PlainFixedLenBinaryEncoder,
                PlainInt96Encoder,
                RleBooleanEncoder,
                RleDictionaryEncoder,
                DeltaBinaryPackedInt32Encoder,
                DeltaBinaryPackedInt64Encoder,
                DeltaLengthByteArrayEncoder,
                DeltaByteArrayEncoder,
                ByteStreamSplitFloatEncoder,
                ByteStreamSplitDoubleEncoder {

    /**
     * Encodes the first {@code n} values from {@code values} and writes the encoded byte sequence to {@code dst}.
     *
     * @return the number of bytes written to {@code dst}
     */
    int encode(T values, int n, WritableByteChannel dst) throws IOException;

    /** The encoding marker for the page header in DataPage V2 mode. */
    Encoding parquetEncoding();

    /**
     * The encoding marker for DataPage V1 mode. Defaults to {@link #parquetEncoding()}; only dictionary encoders
     * override to return the V1-specific {@link Encoding#PLAIN_DICTIONARY} marker.
     */
    default Encoding parquetEncodingV1() {
        return parquetEncoding();
    }
}
