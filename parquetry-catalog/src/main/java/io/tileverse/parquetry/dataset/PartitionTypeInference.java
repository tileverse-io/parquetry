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
package io.tileverse.parquetry.dataset;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.regex.Pattern;

import io.tileverse.parquetry.filter.Value;

/**
 * Infers the type of a Hive partition column from its path values: LONG, then DOUBLE, then DATE (yyyy-MM-dd), else
 * STRING. A leading-zero value (e.g. {@code 08}) stays STRING to keep its textual form; an overflowing integer falls
 * through to DOUBLE then STRING.
 */
final class PartitionTypeInference {

    private static final Pattern LEADING_ZERO = Pattern.compile("0\\d+");

    private PartitionTypeInference() {}

    static Value infer(String raw) {
        return switch (kindOf(raw)) {
            case LONG -> new Value.LongVal(Long.parseLong(raw));
            case DOUBLE -> new Value.DoubleVal(Double.parseDouble(raw));
            case DATE -> new Value.DateVal(LocalDate.parse(raw));
            case STRING -> new Value.StringVal(raw);
        };
    }

    static PartitionKind inferConsistent(List<String> values) {
        if (values.isEmpty()) {
            return PartitionKind.STRING;
        }
        PartitionKind first = kindOf(values.get(0));
        for (String value : values) {
            if (kindOf(value) != first) {
                return PartitionKind.STRING;
            }
        }
        return first;
    }

    static PartitionKind kindOf(String raw) {
        if (LEADING_ZERO.matcher(raw).matches()) {
            return PartitionKind.STRING;
        }
        if (isLong(raw)) {
            return PartitionKind.LONG;
        }
        if (isDouble(raw)) {
            return PartitionKind.DOUBLE;
        }
        if (isDate(raw)) {
            return PartitionKind.DATE;
        }
        return PartitionKind.STRING;
    }

    private static boolean isLong(String raw) {
        try {
            Long.parseLong(raw);
            return true;
        } catch (NumberFormatException notLong) {
            return false;
        }
    }

    private static boolean isDouble(String raw) {
        if (raw.isBlank()) {
            return false;
        }
        try {
            Double.parseDouble(raw);
            return true;
        } catch (NumberFormatException notDouble) {
            return false;
        }
    }

    private static boolean isDate(String raw) {
        try {
            LocalDate.parse(raw);
            return true;
        } catch (DateTimeParseException notDate) {
            return false;
        }
    }
}
