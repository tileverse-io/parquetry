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
package io.tileverse.parquetry.format;

import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_FLOAT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.lang.foreign.ValueLayout;

/**
 * Unaligned little-endian {@link ValueLayout}s for Parquet's fixed-width physical types.
 *
 * <p>Parquet's PLAIN encoding mandates little-endian byte order for {@code INT32}, {@code INT64}, {@code FLOAT}, and
 * {@code DOUBLE} values; the same wire form lands in statistics min/max bytes, in the bloom filter bitset, and in the
 * footer's column index / offset index numeric bounds. Field names match the {@code PrimitiveKind} names so reading
 * sites read naturally: {@code segment.get(ParquetLayouts.INT32, 0)}.
 *
 * <p>{@code BOOLEAN}, {@code INT96}, {@code BYTE_ARRAY}, and {@code FIXED_LEN_BYTE_ARRAY} are not listed here -- they
 * have no single-value layout (variable-width, multi-word, or boolean-packed).
 */
public final class ParquetLayouts {

    /** Unaligned little-endian {@code INT32} layout. */
    public static final ValueLayout.OfInt INT32 = JAVA_INT_UNALIGNED.withOrder(LITTLE_ENDIAN);

    /** Unaligned little-endian {@code INT64} layout. */
    public static final ValueLayout.OfLong INT64 = JAVA_LONG_UNALIGNED.withOrder(LITTLE_ENDIAN);

    /** Unaligned little-endian {@code FLOAT} layout. */
    public static final ValueLayout.OfFloat FLOAT = JAVA_FLOAT_UNALIGNED.withOrder(LITTLE_ENDIAN);

    /** Unaligned little-endian {@code DOUBLE} layout. */
    public static final ValueLayout.OfDouble DOUBLE = JAVA_DOUBLE_UNALIGNED.withOrder(LITTLE_ENDIAN);

    private ParquetLayouts() {
        // utility
    }
}
