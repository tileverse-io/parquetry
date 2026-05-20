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

import io.tileverse.parquetry.schema.geo.projjson.Identifier;

/**
 * Resolves a GeoParquet PROJJSON CRS to a GeoTools {@link CoordinateReferenceSystem}.
 *
 * <p>Resolution order:
 *
 * <ol>
 *   <li>EPSG identifier found in the PROJJSON - decoded via {@code CRS.decode("EPSG:NNNN", true)} (longitudeFirst=true,
 *       matching GeoParquet's lon/lat convention).
 *   <li>No PROJJSON or no recognisable identifier - defaults to {@link DefaultGeographicCRS#WGS84} (OGC:CRS84,
 *       lon/lat).
 * </ol>
 */
final class ProjJsonCrsConverter {

    private ProjJsonCrsConverter() {}

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
