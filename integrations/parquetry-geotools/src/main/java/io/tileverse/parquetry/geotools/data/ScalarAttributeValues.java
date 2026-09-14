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
package io.tileverse.parquetry.geotools.data;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.time.LocalTime;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.filter.DecimalValues;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Reconstruction of a decimal or a time-of-day attribute value from the cell of its column: a decimal from the unscaled
 * integer of an INT32 or INT64 cell or from the two's-complement big-endian bytes of a fixed-length cell, at the
 * column's scale; a time from the count since midnight held by the cell, in the column's unit (an INT32 time column
 * counts milliseconds, an INT64 one micro- or nanoseconds).
 */
final class ScalarAttributeValues {

    private ScalarAttributeValues() {}

    /** The decimal encoded by an integer-backed cell: its {@code unscaled} integer read at the column's scale. */
    static BigDecimal decimal(long unscaled, int scale) {
        return BigDecimal.valueOf(unscaled, scale);
    }

    /** The decimal encoded by a fixed-length cell: its big-endian two's-complement {@code bytes} at {@code scale}. */
    static BigDecimal decimal(MemorySegment bytes, int scale) {
        return DecimalValues.toBigDecimal(bytes, scale);
    }

    /**
     * The time of day encoded by a TIME cell: the count since midnight in {@code value}, read in the column's unit. An
     * INT32 TIME column counts milliseconds whatever its annotation claims; an INT64 one counts in {@code unit}.
     */
    static LocalTime time(long value, PrimitiveKind kind, LogicalType.TimeUnit unit) {
        if (kind == PrimitiveKind.INT32) {
            return TemporalValues.toLocalTime(value, LogicalType.TimeUnit.MILLIS);
        }
        return TemporalValues.toLocalTime(value, unit);
    }
}
