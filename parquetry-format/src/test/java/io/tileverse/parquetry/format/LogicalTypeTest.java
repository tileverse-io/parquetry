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
package io.tileverse.parquetry.format;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class LogicalTypeTest {

    @Test
    void stringTypeIsSingleton() {
        LogicalType.StringType first = new LogicalType.StringType();
        LogicalType.StringType second = new LogicalType.StringType();
        assertThat(first).isEqualTo(second);
    }

    @Test
    void decimalCarriesScaleAndPrecision() {
        LogicalType.Decimal decimal = new LogicalType.Decimal(4, 18);
        assertThat(decimal.scale()).isEqualTo(4);
        assertThat(decimal.precision()).isEqualTo(18);
    }

    @Test
    void timestampHasUnitAndAdjustment() {
        LogicalType.Timestamp ts = new LogicalType.Timestamp(true, LogicalType.TimeUnit.MICROS);
        assertThat(ts.isAdjustedToUTC()).isTrue();
        assertThat(ts.unit()).isEqualTo(LogicalType.TimeUnit.MICROS);
    }

    @Test
    void allVariantsImplementSealedInterface() {
        LogicalType[] all = new LogicalType[] {
            new LogicalType.StringType(),
            new LogicalType.MapType(),
            new LogicalType.ListType(),
            new LogicalType.EnumType(),
            new LogicalType.Decimal(0, 1),
            new LogicalType.DateType(),
            new LogicalType.Time(true, LogicalType.TimeUnit.MILLIS),
            new LogicalType.Timestamp(true, LogicalType.TimeUnit.MILLIS),
            new LogicalType.IntType((byte) 32, true),
            new LogicalType.UnknownType(),
            new LogicalType.JsonType(),
            new LogicalType.BsonType(),
            new LogicalType.UuidType(),
            new LogicalType.Float16Type(),
            new LogicalType.Variant(),
            new LogicalType.Geometry(java.util.Optional.empty()),
            new LogicalType.Geography(java.util.Optional.empty(), java.util.Optional.empty())
        };
        assertThat(all).hasSize(17);
    }
}
