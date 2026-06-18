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
package io.tileverse.parquetry.geotools;

import java.util.Optional;
import java.util.OptionalInt;

import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;

import io.tileverse.parquetry.schema.geo.ParquetCrs;
import io.tileverse.parquetry.schema.geo.projjson.Identifier;

/**
 * Resolves a GeoParquet CRS to a GeoTools {@link CoordinateReferenceSystem}.
 *
 * <p>Resolution order:
 *
 * <ol>
 *   <li>EPSG identifier found in the PROJJSON - decoded via {@code CRS.decode("EPSG:NNNN", true)} (longitudeFirst=true,
 *       matching GeoParquet's lon/lat convention).
 *   <li>No PROJJSON or no recognisable identifier - defaults to {@link DefaultGeographicCRS#WGS84} (OGC:CRS84,
 *       lon/lat).
 * </ol>
 *
 * <p>The {@link #toGeoTools(ParquetCrs)} overload additionally resolves the native Parquet {@code crs} reference forms:
 * an {@code authority:code} reference is decoded through the GeoTools registry, a {@code srid:} reference is treated as
 * an EPSG code ({@code srid:0} is the undefined CRS), and a {@code projjson:} reference (which needs an out-of-band
 * registry) falls back to the default.
 */
final class ProjJsonCrsConverter {

    private ProjJsonCrsConverter() {}

    /**
     * Converts a native Parquet {@code crs} to a GeoTools CRS, resolving the reference forms through the GeoTools
     * registry. Never null; defaults to WGS84 (lon/lat) when resolution fails or the CRS is undefined.
     */
    static CoordinateReferenceSystem toGeoTools(ParquetCrs crs) {
        return switch (crs) {
            case ParquetCrs.Inline(var projjson) -> toGeoTools(Optional.of(projjson));
            case ParquetCrs.AuthorityCode(String authority, String code) -> resolveAuthorityCode(authority, code);
            case ParquetCrs.Srid(long id) -> resolveEpsgSrid(id);
            case ParquetCrs.ProjjsonKey _ -> DefaultGeographicCRS.WGS84;
        };
    }

    /**
     * Resolves an {@code authority:code} reference through the GeoTools registry (longitudeFirst=true). OGC:CRS84 is
     * the GeoParquet default (lon/lat WGS84); any code the registry cannot decode falls back to WGS84.
     */
    private static CoordinateReferenceSystem resolveAuthorityCode(String authority, String code) {
        boolean isOgcCrs84 = "OGC".equalsIgnoreCase(authority) && "CRS84".equalsIgnoreCase(code);
        if (isOgcCrs84) {
            return DefaultGeographicCRS.WGS84;
        }
        try {
            return CRS.decode(authority + ":" + code, true);
        } catch (FactoryException _) {
            return DefaultGeographicCRS.WGS84;
        }
    }

    /**
     * Resolves a {@code srid:} reference as an EPSG code. A non-positive SRID is the undefined CRS and resolves to the
     * WGS84 default, matching the lenient reader posture.
     */
    private static CoordinateReferenceSystem resolveEpsgSrid(long srid) {
        if (srid <= 0) {
            return DefaultGeographicCRS.WGS84;
        }
        try {
            return CRS.decode("EPSG:" + srid, true);
        } catch (FactoryException _) {
            return DefaultGeographicCRS.WGS84;
        }
    }

    /**
     * Converts an optional PROJJSON CRS to a GeoTools CRS.
     *
     * @param projjson the PROJJSON CRS from a GeoParquet geometry column, or empty when the column metadata does not
     *     specify a CRS
     * @return a GeoTools CRS, never null; defaults to WGS84 (lon/lat) when resolution fails
     */
    static CoordinateReferenceSystem toGeoTools(
            Optional<io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem> projjson) {
        if (projjson.isEmpty()) {
            return DefaultGeographicCRS.WGS84;
        }
        Optional<Identifier> id =
                projjson.flatMap(io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem::id);
        if (id.isPresent()) {
            return resolveFromIdentifier(id.get());
        }
        // Non-EPSG PROJJSON is not resolved in this increment; default to CRS84 (the GeoParquet default).
        return DefaultGeographicCRS.WGS84;
    }

    /**
     * Attempts EPSG lookup when the identifier has an EPSG code. Falls back to WGS84 when the EPSG registry is
     * unavailable or the code is unknown.
     */
    private static CoordinateReferenceSystem resolveFromIdentifier(Identifier id) {
        OptionalInt epsg = id.epsgCode();
        if (epsg.isEmpty()) {
            return DefaultGeographicCRS.WGS84;
        }
        try {
            // longitudeFirst=true: GeoParquet geometry columns use lon/lat (easting-first) order.
            return CRS.decode("EPSG:" + epsg.getAsInt(), true);
        } catch (FactoryException e) {
            return DefaultGeographicCRS.WGS84;
        }
    }
}
