/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.filter;

import java.lang.foreign.MemorySegment;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Sealed value type for filter predicates. One case per Parquet primitive shape.
 *
 * <p>Used as the right-hand side of comparison predicates ({@link Predicate.Eq}, {@link Predicate.Lt}, etc.). The ADT
 * is total: predicates can never carry an "untyped" value.
 */
public sealed interface Value
        permits Value.BoolVal,
                Value.IntVal,
                Value.LongVal,
                Value.FloatVal,
                Value.DoubleVal,
                Value.BinaryVal,
                Value.StringVal,
                Value.DateVal,
                Value.TimestampVal,
                Value.UuidVal {

    record BoolVal(boolean value) implements Value {}

    record IntVal(int value) implements Value {}

    record LongVal(long value) implements Value {}

    record FloatVal(float value) implements Value {}

    record DoubleVal(double value) implements Value {}

    record BinaryVal(MemorySegment value) implements Value {
        public BinaryVal {
            value = value.isReadOnly() ? value : value.asReadOnly();
        }
    }

    record StringVal(String value) implements Value {}

    record DateVal(LocalDate value) implements Value {}

    record TimestampVal(LocalDateTime value, boolean adjustedToUTC) implements Value {}

    /**
     * A UUID predicate value. Compared in Parquet's unsigned byte order for FIXED_LEN_BYTE_ARRAY columns (via
     * {@link io.tileverse.parquetry.schema.UuidConverter#compareSegmentToUuid}), NOT {@link UUID#compareTo}, which is
     * signed and would disagree with file statistics whenever a long's high bit differs.
     */
    record UuidVal(UUID value) implements Value {}
}
