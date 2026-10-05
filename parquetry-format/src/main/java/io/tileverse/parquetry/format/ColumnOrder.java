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
package io.tileverse.parquetry.format;

/**
 * Thrift {@code union ColumnOrder}: the order followed by a column's {@code min_value}/{@code max_value} statistics and
 * by the page bounds of its column index.
 *
 * <p>A reader that does not support the order of a column ignores that column's min and max statistics. A union case
 * added by a newer writer reads as {@link Unknown}, keeping its field id; a re-write emits the case with an empty
 * payload. A union naming no case reads as {@link Unknown} too, with {@link Unknown#NO_CASE}: it defines no order.
 */
public sealed interface ColumnOrder
        permits ColumnOrder.TypeDefined,
                ColumnOrder.Ieee754TotalOrder,
                ColumnOrder.Int96TimestampOrder,
                ColumnOrder.Unknown {

    /** The order defined by the column's logical type, or by its physical type when it has none (union field 1). */
    record TypeDefined() implements ColumnOrder {}

    /** IEEE 754 {@code totalOrder} for FLOAT, DOUBLE and FLOAT16 columns (union field 2). */
    record Ieee754TotalOrder() implements ColumnOrder {}

    /** Chronological order of INT96 timestamps (union field 3). */
    record Int96TimestampOrder() implements ColumnOrder {}

    /**
     * A union case unknown to this reader.
     *
     * @param fieldId the Thrift field id of the case, {@link #NO_CASE} for a union naming no case
     */
    record Unknown(short fieldId) implements ColumnOrder {

        /** The field id of a union naming no case; a Thrift field id is positive. */
        public static final short NO_CASE = 0;
    }
}
