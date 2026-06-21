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
package io.tileverse.parquetry.dataset;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.function.Function;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Parses a Hive partition value string into a typed {@link Value}, using the bound column's logical type first and its
 * physical {@link PrimitiveKind} as a fallback.
 *
 * <p>Parsing never throws. A value the column cannot represent (an unparseable number, an unsupported physical kind, or
 * Hive's null-partition sentinel) yields {@link Optional#empty()}, and the caller drops the pruning stat for that
 * column rather than aborting the catalog open with a wrong or impossible bound.
 */
final class PartitionValueParser {

    /** Hive writes this literal as the path segment for a null partition value. */
    static final String HIVE_NULL_PARTITION = "__HIVE_DEFAULT_PARTITION__";

    private PartitionValueParser() {}

    /** Whether {@code raw} is Hive's null-partition sentinel path segment. */
    static boolean isNullPartition(String raw) {
        return HIVE_NULL_PARTITION.equals(raw);
    }

    static Optional<Value> parse(String raw, PrimitiveKind kind, Optional<LogicalType> logicalType) {
        if (raw == null) {
            return Optional.empty();
        }
        if (isNullPartition(raw)) {
            return Optional.empty();
        }
        if (logicalType.orElse(null) instanceof LogicalType.DateType) {
            return parseDate(raw);
        }
        return parseByPhysicalKind(raw, kind);
    }

    private static Optional<Value> parseDate(String raw) {
        try {
            return Optional.of(new Value.DateVal(LocalDate.parse(raw)));
        } catch (DateTimeParseException notADate) {
            return Optional.empty();
        }
    }

    private static Optional<Value> parseByPhysicalKind(String raw, PrimitiveKind kind) {
        return switch (kind) {
            case BOOLEAN -> parseBoolean(raw);
            case INT32 -> tryParse(raw, Integer::parseInt).map(Value.IntVal::new);
            case INT64 -> tryParse(raw, Long::parseLong).map(Value.LongVal::new);
            case FLOAT -> tryParse(raw, Float::parseFloat).map(Value.FloatVal::new);
            case DOUBLE -> tryParse(raw, Double::parseDouble).map(Value.DoubleVal::new);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> Optional.of(new Value.StringVal(raw));
            case INT96 -> Optional.empty();
        };
    }

    private static Optional<Value> parseBoolean(String raw) {
        if ("true".equalsIgnoreCase(raw) || "1".equals(raw)) {
            return Optional.of(new Value.BoolVal(true));
        }
        if ("false".equalsIgnoreCase(raw) || "0".equals(raw)) {
            return Optional.of(new Value.BoolVal(false));
        }
        return Optional.empty();
    }

    private static <T> Optional<T> tryParse(String raw, Function<String, T> parser) {
        try {
            return Optional.of(parser.apply(raw));
        } catch (NumberFormatException notANumber) {
            return Optional.empty();
        }
    }
}
