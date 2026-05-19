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

import java.util.Optional;

/**
 * Parquet logical-type annotation (Thrift union {@code LogicalType}).
 *
 * <p>Sealed interface with one record per Thrift-union variant. Records that need no payload (e.g. {@link StringType})
 * are empty records to give every variant the same shape.
 *
 * <p>Variant {@link VariantStub} is a stub initially; it gains the variant type carrier later. Declared here so the
 * sealed-type list is final from the start.
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
                LogicalType.Geometry,
                LogicalType.Geography {

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

    // Stub (filled in later)
    record VariantStub() implements LogicalType {}

    /**
     * GeoParquet 2.0 {@code GEOMETRY} logical type.
     *
     * @param crs PROJJSON document as a raw string when present; {@link Optional#empty()} means "use the GeoParquet
     *     spec default" (typically OGC:CRS84). parquetry exposes the raw JSON so consumers can plug in their own
     *     PROJJSON parser.
     */
    record Geometry(Optional<String> crs) implements LogicalType {}

    /**
     * GeoParquet 2.0 {@code GEOGRAPHY} logical type.
     *
     * @param crs PROJJSON document as a raw string when present; {@link Optional#empty()} means "use the GeoParquet
     *     spec default".
     * @param algorithm edge interpolation algorithm; {@link Optional#empty()} means "use the spec default"
     *     ({@code SPHERICAL}).
     */
    record Geography(Optional<String> crs, Optional<EdgeInterpolationAlgorithm> algorithm) implements LogicalType {}
}
