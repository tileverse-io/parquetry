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
package io.tileverse.parquetry.geotools.data;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Optional;

import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;

import io.tileverse.parquetry.schema.geo.ParquetCrs;
import io.tileverse.parquetry.schema.geo.geoparquet.ColumnCrs;
import io.tileverse.parquetry.schema.geo.projjson.Identifier;

/**
 * Resolves a GeoParquet CRS to a GeoTools {@link CoordinateReferenceSystem}.
 *
 * <p>A column that declares no CRS of its own uses the GeoParquet default, OGC:CRS84. Every other case resolves through
 * an identifier: OGC:CRS84 is that default itself, and any other authority and code goes to the GeoTools registry,
 * decoded with {@code longitudeFirst=true} to match the lon/lat order of GeoParquet geometry columns.
 *
 * <p>Resolution returns empty when the CRS is unknown, when it has no identifier, or when the registry cannot decode
 * it, and logs what it could not resolve. A caller then assigns no CRS. Standing in with WGS 84 would state degrees for
 * data that may be in metres: a column in ESRI:102003 is in metres, and reading it as WGS 84 misplaces every coordinate
 * without anything saying so. Resolving a CRS defined inline with no identifier at all needs a PROJJSON-to-GeoTools
 * conversion this package does not have.
 */
final class ProjJsonCrsConverter {

    private static final Logger LOGGER = System.getLogger(ProjJsonCrsConverter.class.getName());

    private ProjJsonCrsConverter() {}

    /**
     * Resolves what a GeoParquet column says about its CRS. The spec default stands in for an absent declaration; an
     * unknown or unresolvable CRS resolves to empty.
     */
    static Optional<CoordinateReferenceSystem> toGeoTools(ColumnCrs declaration) {
        return switch (declaration) {
            case ColumnCrs.Defaulted _ -> Optional.of(DefaultGeographicCRS.WGS84);
            case ColumnCrs.Unknown _ -> unresolved("the column declares its CRS unknown");
            case ColumnCrs.Defined defined ->
                fromIdentifier(defined.crs().id(), defined.crs().name());
        };
    }

    /**
     * Resolves a native Parquet {@code crs} reference. An {@code authority:code} goes through the GeoTools registry, a
     * {@code srid:} reference is read as an EPSG code, and a {@code projjson:} reference needs an out-of-band registry,
     * unavailable in this package.
     */
    static Optional<CoordinateReferenceSystem> toGeoTools(ParquetCrs crs) {
        return switch (crs) {
            case ParquetCrs.Inline inline ->
                fromIdentifier(inline.projjson().id(), inline.projjson().name());
            case ParquetCrs.AuthorityCode(String authority, String code) -> fromAuthorityCode(authority, code);
            case ParquetCrs.Srid(long id) -> fromSrid(id);
            case ParquetCrs.ProjjsonKey(String key) -> unresolved("projjson:" + key + " needs an out of band registry");
        };
    }

    private static Optional<CoordinateReferenceSystem> fromAuthorityCode(String authority, String code) {
        boolean isOgcCrs84 = "OGC".equalsIgnoreCase(authority) && "CRS84".equalsIgnoreCase(code);
        if (isOgcCrs84) {
            return Optional.of(DefaultGeographicCRS.WGS84);
        }
        return decode(authority + ":" + code);
    }

    private static Optional<CoordinateReferenceSystem> fromSrid(long srid) {
        if (srid <= 0) {
            return unresolved("srid:" + srid + " is the undefined CRS");
        }
        return decode("EPSG:" + srid);
    }

    private static Optional<CoordinateReferenceSystem> fromIdentifier(
            Optional<Identifier> id, Optional<String> crsName) {
        if (id.isEmpty()) {
            return unresolved("the inline PROJJSON of " + crsName.orElse("an unnamed CRS") + " has no identifier");
        }
        return fromAuthorityCode(id.get().authority(), id.get().code());
    }

    /** Decodes with {@code longitudeFirst=true}: GeoParquet geometry columns use lon/lat (easting-first) order. */
    private static Optional<CoordinateReferenceSystem> decode(String code) {
        try {
            return Optional.of(CRS.decode(code, true));
        } catch (FactoryException e) {
            return unresolved(code + " is not in the GeoTools registry: " + e.getMessage());
        }
    }

    private static Optional<CoordinateReferenceSystem> unresolved(String reason) {
        LOGGER.log(Level.WARNING, "No coordinate reference system assigned: {0}", reason);
        return Optional.empty();
    }
}
