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

import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;

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

    /**
     * Sub-second precision for {@link Time} and {@link Timestamp}; mirror of the {@code TimeUnit} union in
     * {@code parquet.thrift}.
     *
     * <p>The Thrift schema models this as a one-of-N union of empty structs:
     *
     * <pre>
     * union TimeUnit {
     *   1: MilliSeconds MILLIS
     *   2: MicroSeconds MICROS
     *   3: NanoSeconds NANOS
     * }
     * </pre>
     *
     * <p>{@code TimeUnit} is structurally a Thrift <em>union</em>, not a Thrift enum: each variant is identified on the
     * wire by its Thrift compact-protocol <em>field id</em> (delta-encoded inside the union struct's field header), not
     * by an assigned i32 enum value. Each Java constant therefore carries its field id in {@link #fieldId()};
     * deserializers resolve incoming ids via {@link #valueOf(int)}.
     *
     * <p>Unlike {@link EdgeInterpolationAlgorithm}, this lookup is fail-fast on unknown ids: silently defaulting to
     * {@link #MILLIS} when a future Parquet writer emits a new variant (e.g. picoseconds) would yield timestamps wrong
     * by many orders of magnitude. Throwing surfaces the unknown variant at the deserialize boundary where the caller
     * can decide to fail the read or skip the column.
     */
    enum TimeUnit {
        MILLIS(1),
        MICROS(2),
        NANOS(3);

        private final int fieldId;

        TimeUnit(int fieldId) {
            this.fieldId = fieldId;
        }

        /** Thrift compact-protocol field id of this variant within the {@code TimeUnit} union. */
        public int fieldId() {
            return fieldId;
        }

        /**
         * Returns the variant whose {@link #fieldId()} equals {@code fieldId}. Compiles to a {@code tableswitch}
         * bytecode (O(1) lookup, no allocations).
         *
         * @throws UnknownVariantException if no defined variant carries that field id - see the enum javadoc on why
         *     this lookup is intentionally fail-fast rather than forward-compat-tolerant.
         */
        public static TimeUnit valueOf(int fieldId) {
            return switch (fieldId) {
                case 1 -> MILLIS;
                case 2 -> MICROS;
                case 3 -> NANOS;
                default -> throw new UnknownVariantException("Unknown TimeUnit field id: " + fieldId);
            };
        }
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
     * @param crs typed PROJJSON coordinate reference system when present; {@link Optional#empty()} means "use the
     *     GeoParquet spec default" (typically OGC:CRS84). PROJJSON is parsed eagerly at footer-read time;
     *     {@link CoordinateReferenceSystem.Unknown} surfaces any {@code type} discriminator not yet modeled, and a
     *     malformed PROJJSON string degrades to {@link Optional#empty()} so the column is still readable.
     */
    record Geometry(Optional<CoordinateReferenceSystem> crs) implements LogicalType {}

    /**
     * GeoParquet 2.0 {@code GEOGRAPHY} logical type.
     *
     * @param crs typed PROJJSON coordinate reference system when present; {@link Optional#empty()} means "use the
     *     GeoParquet spec default".
     * @param algorithm edge interpolation algorithm; {@link Optional#empty()} means "use the spec default"
     *     ({@code SPHERICAL}).
     */
    record Geography(Optional<CoordinateReferenceSystem> crs, Optional<EdgeInterpolationAlgorithm> algorithm)
            implements LogicalType {}
}
