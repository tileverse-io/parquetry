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
package io.tileverse.parquetry.iceberg;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.tileverse.parquetry.format.EdgeInterpolationAlgorithm;
import io.tileverse.parquetry.format.LogicalType.TimeUnit;
import io.tileverse.parquetry.schema.geo.ParquetCrs;

/**
 * A primitive Iceberg field type, as an Iceberg schema serializes it in a field's {@code type} token: a bare name
 * ({@code long}, {@code timestamptz_ns}, {@code uuid}) or a parameterized form ({@code decimal(9, 2)},
 * {@code fixed[16]}, {@code geography(OGC:CRS84, karney)}).
 *
 * <p>Every reader decision that depends on a field's type switches over this sealed family: presenting the field as a
 * Parquet leaf, decoding its manifest bounds, reading its equality-delete cells, converting its partition values,
 * parsing its {@code initial-default}, and null-filling it in a data file written before the field existed. A
 * parameterized type keeps its parameters on the member (a decimal's precision and scale, a fixed type's length, a
 * timestamp's adjustment and unit, a geometry's CRS and a geography's edge algorithm), which is where each consumer
 * reads them.
 *
 * <p>Nested types (struct, list, map) are not primitive types and never reach {@link #parse(String)}; the
 * {@code variant} token is rejected there, keeping "unsupported at open" for what stays unsupported. A geometry or
 * geography token without parameters presents Iceberg's spec default CRS {@code OGC:CRS84}; an explicit but
 * unclassifiable CRS presents no CRS, keeping the column readable; an inline PROJJSON CRS is rejected. {@link #token()}
 * renders the retained classification, hence a default or absent CRS renders as the bare kind.
 */
sealed interface IcebergType
        permits IcebergType.BoolType,
                IcebergType.IntType,
                IcebergType.LongType,
                IcebergType.FloatType,
                IcebergType.DoubleType,
                IcebergType.DateType,
                IcebergType.StringType,
                IcebergType.BinaryType,
                IcebergType.UuidType,
                IcebergType.TimestampType,
                IcebergType.TimeType,
                IcebergType.DecimalType,
                IcebergType.FixedType,
                IcebergType.GeometryType,
                IcebergType.GeographyType,
                IcebergType.UnknownType {

    /** The Iceberg type name for geometry: the bare token, and the prefix of a parameterized one. */
    String GEOMETRY_TOKEN = "geometry";

    /** The Iceberg type name for geography: the bare token, and the prefix of a parameterized one. */
    String GEOGRAPHY_TOKEN = "geography";

    /** Iceberg's spec default CRS for a geometry or geography type declared without parameters. */
    Optional<ParquetCrs> DEFAULT_GEO_CRS = ParquetCrs.reference("OGC:CRS84");

    int MAX_DECIMAL_PRECISION = 38;

    Pattern DECIMAL_TOKEN = Pattern.compile("decimal\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)");

    Pattern FIXED_TOKEN = Pattern.compile("fixed\\[\\s*(\\d+)\\s*\\]");

    /** The serialized Iceberg type token of this type, as a schema field's {@code type} declares it. */
    String token();

    record BoolType() implements IcebergType {
        @Override
        public String token() {
            return "boolean";
        }
    }

    record IntType() implements IcebergType {
        @Override
        public String token() {
            return "int";
        }
    }

    record LongType() implements IcebergType {
        @Override
        public String token() {
            return "long";
        }
    }

    record FloatType() implements IcebergType {
        @Override
        public String token() {
            return "float";
        }
    }

    record DoubleType() implements IcebergType {
        @Override
        public String token() {
            return "double";
        }
    }

    /** A calendar date, stored as days since the epoch. */
    record DateType() implements IcebergType {
        @Override
        public String token() {
            return "date";
        }
    }

    record StringType() implements IcebergType {
        @Override
        public String token() {
            return "string";
        }
    }

    record BinaryType() implements IcebergType {
        @Override
        public String token() {
            return "binary";
        }
    }

    record UuidType() implements IcebergType {
        @Override
        public String token() {
            return "uuid";
        }
    }

    /**
     * A timestamp at microsecond or nanosecond precision, with or without UTC adjustment: the four timestamp tokens.
     */
    record TimestampType(boolean adjustToUtc, TimeUnit unit) implements IcebergType {
        public TimestampType {
            if (unit != TimeUnit.MICROS && unit != TimeUnit.NANOS) {
                throw new IllegalArgumentException(
                        "an Iceberg timestamp is microsecond or nanosecond precision, not " + unit);
            }
        }

        @Override
        public String token() {
            String base = adjustToUtc ? "timestamptz" : "timestamp";
            return unit == TimeUnit.NANOS ? base + "_ns" : base;
        }
    }

    /** A time of day; Iceberg stores it as microseconds since midnight. */
    record TimeType() implements IcebergType {
        @Override
        public String token() {
            return "time";
        }
    }

    record DecimalType(int precision, int scale) implements IcebergType {
        public DecimalType {
            if (precision < 1 || precision > MAX_DECIMAL_PRECISION) {
                throw new IllegalArgumentException("decimal precision must be in [1, 38], got " + precision);
            }
            if (scale < 0 || scale > precision) {
                throw new IllegalArgumentException("decimal scale must be in [0, precision], got " + scale);
            }
        }

        @Override
        public String token() {
            return "decimal(" + precision + ", " + scale + ")";
        }
    }

    record FixedType(int length) implements IcebergType {
        public FixedType {
            if (length < 1) {
                throw new IllegalArgumentException("fixed length must be positive, got " + length);
            }
        }

        @Override
        public String token() {
            return "fixed[" + length + "]";
        }
    }

    record GeometryType(Optional<ParquetCrs> crs) implements IcebergType {
        public GeometryType {
            Objects.requireNonNull(crs, "crs");
        }

        @Override
        public String token() {
            return geoToken(GEOMETRY_TOKEN, crs, Optional.empty());
        }
    }

    record GeographyType(Optional<ParquetCrs> crs, Optional<EdgeInterpolationAlgorithm> algorithm)
            implements IcebergType {
        public GeographyType {
            Objects.requireNonNull(crs, "crs");
            Objects.requireNonNull(algorithm, "algorithm");
        }

        @Override
        public String token() {
            return geoToken(GEOGRAPHY_TOKEN, crs, algorithm);
        }
    }

    /** The v3 always-null type. */
    record UnknownType() implements IcebergType {
        @Override
        public String token() {
            return "unknown";
        }
    }

    /**
     * Parses a schema field's {@code type} token; a nested, unknown, or malformed token is an
     * {@link IcebergFormatException}.
     */
    static IcebergType parse(String token) {
        Objects.requireNonNull(token, "token");
        String trimmed = token.strip();
        return switch (trimmed) {
            case "boolean" -> new BoolType();
            case "int" -> new IntType();
            case "long" -> new LongType();
            case "float" -> new FloatType();
            case "double" -> new DoubleType();
            case "date" -> new DateType();
            case "time" -> new TimeType();
            case "timestamp" -> new TimestampType(false, TimeUnit.MICROS);
            case "timestamptz" -> new TimestampType(true, TimeUnit.MICROS);
            case "timestamp_ns" -> new TimestampType(false, TimeUnit.NANOS);
            case "timestamptz_ns" -> new TimestampType(true, TimeUnit.NANOS);
            case "string" -> new StringType();
            case "uuid" -> new UuidType();
            case "binary" -> new BinaryType();
            case "unknown" -> new UnknownType();
            default -> parseParameterized(trimmed, token);
        };
    }

    private static IcebergType parseParameterized(String trimmed, String original) {
        if (trimmed.startsWith("decimal")) {
            return parseDecimal(trimmed, original);
        }
        if (trimmed.startsWith("fixed")) {
            return parseFixed(trimmed, original);
        }
        if (trimmed.startsWith(GEOMETRY_TOKEN) || trimmed.startsWith(GEOGRAPHY_TOKEN)) {
            return parseGeo(trimmed, original);
        }
        throw new IcebergFormatException("unsupported Iceberg field type: " + original);
    }

    private static IcebergType parseDecimal(String trimmed, String original) {
        Matcher matcher = DECIMAL_TOKEN.matcher(trimmed);
        if (!matcher.matches()) {
            throw new IcebergFormatException("malformed decimal type token: " + original);
        }
        int precision = parameter(matcher.group(1), original);
        int scale = parameter(matcher.group(2), original);
        if (precision < 1 || precision > MAX_DECIMAL_PRECISION || scale < 0 || scale > precision) {
            throw new IcebergFormatException(
                    "decimal precision must be in [1, 38] and scale in [0, precision]: " + original);
        }
        return new DecimalType(precision, scale);
    }

    private static IcebergType parseFixed(String trimmed, String original) {
        Matcher matcher = FIXED_TOKEN.matcher(trimmed);
        if (!matcher.matches()) {
            throw new IcebergFormatException("malformed fixed type token: " + original);
        }
        int length = parameter(matcher.group(1), original);
        if (length < 1) {
            throw new IcebergFormatException("fixed length must be positive: " + original);
        }
        return new FixedType(length);
    }

    private static int parameter(String digits, String original) {
        try {
            return Integer.parseInt(digits);
        } catch (NumberFormatException e) {
            throw new IcebergFormatException("type parameter out of range: " + original, e);
        }
    }

    private static IcebergType parseGeo(String trimmed, String original) {
        boolean geography = trimmed.startsWith(GEOGRAPHY_TOKEN);
        String kind = geography ? GEOGRAPHY_TOKEN : GEOMETRY_TOKEN;
        if (!trimmed.equals(kind) && !trimmed.startsWith(kind + "(")) {
            throw new IcebergFormatException("unsupported Iceberg field type: " + original);
        }
        int open = trimmed.indexOf('(');
        if (open < 0) {
            return geography ? new GeographyType(DEFAULT_GEO_CRS, Optional.empty()) : new GeometryType(DEFAULT_GEO_CRS);
        }
        String parameters = geoParameters(trimmed, open, original);
        if (parameters.startsWith("{")) {
            throw new IcebergFormatException(
                    "inline PROJJSON CRS in an Iceberg geometry type token is not supported: " + original);
        }
        int comma = parameters.indexOf(',');
        if (comma >= 0 && !geography) {
            throw new IcebergFormatException("geometry type does not take an algorithm parameter: " + original);
        }
        String crsToken =
                comma < 0 ? parameters : parameters.substring(0, comma).strip();
        if (crsToken.isEmpty()) {
            throw new IcebergFormatException("missing CRS in geometry type parameters: " + original);
        }
        Optional<ParquetCrs> crs = ParquetCrs.reference(crsToken);
        if (!geography) {
            return new GeometryType(crs);
        }
        Optional<EdgeInterpolationAlgorithm> algorithm = comma < 0
                ? Optional.empty()
                : parseAlgorithm(parameters.substring(comma + 1).strip());
        return new GeographyType(crs, algorithm);
    }

    private static String geoParameters(String trimmed, int open, String original) {
        if (!trimmed.endsWith(")")) {
            throw new IcebergFormatException("malformed geometry type token: " + original);
        }
        String parameters = trimmed.substring(open + 1, trimmed.length() - 1).strip();
        if (parameters.isEmpty()) {
            throw new IcebergFormatException("empty geometry type parameters: " + original);
        }
        return parameters;
    }

    /** An algorithm name that Iceberg does not define presents no algorithm rather than failing the open. */
    private static Optional<EdgeInterpolationAlgorithm> parseAlgorithm(String token) {
        if (token.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(EdgeInterpolationAlgorithm.valueOf(token.toUpperCase(Locale.ROOT)));
        } catch (IllegalArgumentException _) {
            return Optional.empty();
        }
    }

    private static String geoToken(
            String kind, Optional<ParquetCrs> crs, Optional<EdgeInterpolationAlgorithm> algorithm) {
        Optional<String> crsToken =
                crs.filter(c -> !DEFAULT_GEO_CRS.map(c::equals).orElse(false)).map(IcebergType::crsToken);
        if (crsToken.isEmpty() && algorithm.isEmpty()) {
            return kind;
        }
        StringBuilder parameters = new StringBuilder(crsToken.orElse("OGC:CRS84"));
        algorithm.ifPresent(a -> parameters.append(", ").append(a.name().toLowerCase(Locale.ROOT)));
        return kind + "(" + parameters + ")";
    }

    private static String crsToken(ParquetCrs crs) {
        return switch (crs) {
            case ParquetCrs.AuthorityCode(String authority, String code) -> authority + ":" + code;
            case ParquetCrs.Srid(long id) -> "srid:" + id;
            case ParquetCrs.ProjjsonKey(String key) -> "projjson:" + key;
            case ParquetCrs.Inline _ ->
                throw new IllegalStateException("an inline PROJJSON CRS is rejected at parse and has no token");
        };
    }
}
