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
import java.util.Optional;

import io.tileverse.parquetry.format.EdgeInterpolationAlgorithm;
import io.tileverse.parquetry.schema.geo.ParquetCrs;

/**
 * A parsed Iceberg {@code geometry} / {@code geography} type token: its base kind, coordinate reference system, and
 * (for geography) edge-interpolation algorithm.
 *
 * <p>Iceberg v3 serializes a geometry type as {@code geometry} or {@code geometry(<crs>)} and a geography type as
 * {@code geography}, {@code geography(<crs>)}, or {@code geography(<crs>, <algorithm>)}. The CRS token is classified
 * through {@link ParquetCrs#reference(String)}, the same lenient classifier the plain-GeoParquet reader uses. A token
 * with no parenthesized parameters presents Iceberg's spec default {@code OGC:CRS84}; an explicit but unclassifiable
 * CRS presents no CRS, keeping the column readable. A structurally malformed token (a missing closing parenthesis or
 * empty parentheses) is rejected, as is a geometry token with an algorithm parameter and a token with an empty CRS
 * slot. An inline PROJJSON ({@code {...}}) CRS in a type token is rejected; an Iceberg CRS is expected as a reference
 * such as OGC:CRS84, srid:N, EPSG:code, or projjson:key.
 */
record IcebergGeometryType(String baseKind, Optional<ParquetCrs> crs, Optional<EdgeInterpolationAlgorithm> algorithm) {

    private static final Optional<ParquetCrs> DEFAULT_CRS = ParquetCrs.reference("OGC:CRS84");

    private static final String GEOMETRY = "geometry";

    private static final String GEOGRAPHY = "geography";

    static boolean isGeometryToken(String type) {
        String trimmed = type.strip();
        return trimmed.equals(GEOMETRY)
                || trimmed.startsWith(GEOMETRY + "(")
                || trimmed.equals(GEOGRAPHY)
                || trimmed.startsWith(GEOGRAPHY + "(");
    }

    static IcebergGeometryType parse(String token) {
        String trimmed = token.strip();
        String baseKind = baseKindOf(trimmed, token);
        int open = trimmed.indexOf('(');
        if (open < 0) {
            return new IcebergGeometryType(baseKind, DEFAULT_CRS, Optional.empty());
        }
        String parameters = parametersOf(trimmed, open, token);
        if (parameters.startsWith("{")) {
            throw new IcebergFormatException(
                    "inline PROJJSON CRS in an Iceberg geometry type token is not supported: " + token);
        }
        int comma = parameters.indexOf(',');
        if (comma >= 0 && baseKind.equals(GEOMETRY)) {
            throw new IcebergFormatException("geometry type does not take an algorithm parameter: " + token);
        }
        String crsToken =
                comma < 0 ? parameters : parameters.substring(0, comma).strip();
        if (crsToken.isEmpty()) {
            throw new IcebergFormatException("missing CRS in geometry type parameters: " + token);
        }
        Optional<ParquetCrs> crs = ParquetCrs.reference(crsToken);
        Optional<EdgeInterpolationAlgorithm> algorithm = comma < 0
                ? Optional.empty()
                : parseAlgorithm(parameters.substring(comma + 1).strip());
        return new IcebergGeometryType(baseKind, crs, algorithm);
    }

    private static String baseKindOf(String trimmed, String original) {
        if (trimmed.equals(GEOMETRY) || trimmed.startsWith(GEOMETRY + "(")) {
            return GEOMETRY;
        }
        if (trimmed.equals(GEOGRAPHY) || trimmed.startsWith(GEOGRAPHY + "(")) {
            return GEOGRAPHY;
        }
        throw new IcebergFormatException("not a geometry type token: " + original);
    }

    private static String parametersOf(String trimmed, int open, String original) {
        if (!trimmed.endsWith(")")) {
            throw new IcebergFormatException("malformed geometry type token: " + original);
        }
        String parameters = trimmed.substring(open + 1, trimmed.length() - 1).strip();
        if (parameters.isEmpty()) {
            throw new IcebergFormatException("empty geometry type parameters: " + original);
        }
        return parameters;
    }

    private static Optional<EdgeInterpolationAlgorithm> parseAlgorithm(String token) {
        if (token.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(EdgeInterpolationAlgorithm.valueOf(token.toUpperCase(Locale.ROOT)));
        } catch (IllegalArgumentException unrecognized) {
            return Optional.empty();
        }
    }
}
