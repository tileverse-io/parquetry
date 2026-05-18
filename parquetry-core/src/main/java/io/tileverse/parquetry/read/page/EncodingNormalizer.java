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

import io.tileverse.parquetry.format.enums.Encoding;

/**
 * Normalizes encoding markers stamped on data pages.
 *
 * <p>Parquet 1.x writers tag dictionary-encoded data pages with {@link Encoding#PLAIN_DICTIONARY} while 2.x writers use
 * {@link Encoding#RLE_DICTIONARY}. Both refer to the same on-disk shape (RLE-bit-packed dictionary indices); the
 * difference is purely metadata. The {@code DecoderFactory} dispatches both to the same decoder, but downstream code
 * (the column reader, the {@link DecodedPage} record) sees the value the writer recorded. This class collapses the two
 * to {@link Encoding#RLE_DICTIONARY} at decode time so callers can switch on one case.
 *
 * <p>Dictionary <em>pages</em> still use {@link Encoding#PLAIN} and are not affected by this normalization.
 */
final class EncodingNormalizer {

    private EncodingNormalizer() {}

    /**
     * Returns {@link Encoding#RLE_DICTIONARY} when {@code encoding} is {@link Encoding#PLAIN_DICTIONARY}; otherwise
     * returns the input unchanged.
     */
    static Encoding normalize(Encoding encoding) {
        return encoding == Encoding.PLAIN_DICTIONARY ? Encoding.RLE_DICTIONARY : encoding;
    }
}
