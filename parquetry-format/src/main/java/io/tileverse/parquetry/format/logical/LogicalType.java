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
package io.tileverse.parquetry.format.logical;

/**
 * Parquet logical-type annotation (Thrift union {@code LogicalType}).
 *
 * <p>Sealed interface with one record per Thrift-union variant. Records that need no payload (e.g. {@link StringType})
 * are empty records to give every variant the same shape.
 *
 * <p>Variants {@link VariantStub}, {@link GeometryStub}, {@link GeographyStub} are stubs initially;
 * GeometryStub/GeographyStub gain CRS+algorithm fields later, and VariantStub gains the variant type carrier later.
 * They're declared here so the sealed-type list is final from the start.
 */
public sealed interface LogicalType
        permits LogicalType.StringType,
                LogicalType.MapType,
                LogicalType.ListType,
                LogicalType.EnumType,
                LogicalType.Decimal,
                LogicalType.DateType,
                LogicalType.Time,
                LogicalType.Timestamp,
                LogicalType.IntType,
                LogicalType.UnknownType,
                LogicalType.JsonType,
                LogicalType.BsonType,
                LogicalType.UuidType,
                LogicalType.Float16Type,
                LogicalType.VariantStub,
                LogicalType.GeometryStub,
                LogicalType.GeographyStub {

    enum TimeUnit {
        MILLIS,
        MICROS,
        NANOS
    }

    record StringType() implements LogicalType {}

    record MapType() implements LogicalType {}

    record ListType() implements LogicalType {}

    record EnumType() implements LogicalType {}

    record DateType() implements LogicalType {}

    record UnknownType() implements LogicalType {}

    record JsonType() implements LogicalType {}

    record BsonType() implements LogicalType {}

    record UuidType() implements LogicalType {}

    record Float16Type() implements LogicalType {}

    record Decimal(int scale, int precision) implements LogicalType {}

    record Time(boolean isAdjustedToUTC, TimeUnit unit) implements LogicalType {}

    record Timestamp(boolean isAdjustedToUTC, TimeUnit unit) implements LogicalType {}

    record IntType(byte bitWidth, boolean isSigned) implements LogicalType {}

    // Stubs (filled in later)
    record VariantStub() implements LogicalType {} // a future release will add VariantType

    record GeometryStub() implements LogicalType {} // a future release will add Geometry(crs, algorithm)

    record GeographyStub() implements LogicalType {} // a future release will add Geography(crs, algorithm)
}
