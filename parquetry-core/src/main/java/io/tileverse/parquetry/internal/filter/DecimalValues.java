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
package io.tileverse.parquetry.internal.filter;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Decodes a Parquet DECIMAL FIXED_LEN_BYTE_ARRAY cell - a signed two's-complement big-endian unscaled integer at the
 * column scale - to a primitive {@code long} (at most 8 bytes, no allocation), a {@link BigInteger} (any width), or a
 * {@link BigDecimal}.
 */
public final class DecimalValues {

    private DecimalValues() {}

    /** Sign-extends a big-endian two's-complement segment of at most 8 bytes to a {@code long}. */
    public static long signedLong(MemorySegment segment) {
        long size = segment.byteSize();
        long value = (size > 0 && segment.get(JAVA_BYTE, 0) < 0) ? -1L : 0L;
        for (long i = 0; i < size; i++) {
            value = (value << 8) | (segment.get(JAVA_BYTE, i) & 0xffL);
        }
        return value;
    }

    /** The signed big-endian two's-complement integer a segment encodes (any width). */
    public static BigInteger signedBig(MemorySegment segment) {
        return new BigInteger(segment.toArray(JAVA_BYTE));
    }

    /** The decimal a segment encodes at {@code scale}. */
    public static BigDecimal toBigDecimal(MemorySegment segment, int scale) {
        return new BigDecimal(signedBig(segment), scale);
    }
}
