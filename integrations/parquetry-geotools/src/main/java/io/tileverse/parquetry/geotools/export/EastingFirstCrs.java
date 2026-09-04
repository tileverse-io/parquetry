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
package io.tileverse.parquetry.geotools.export;

import java.util.Optional;

import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystems;

/**
 * The PROJJSON CRS an exported geometry column declares.
 *
 * <p>Geometries are stored easting-first (see {@link EastingFirstFeatures}), and WGS84 has an identifier meaning
 * exactly that: OGC:CRS84. Declaring it, in place of EPSG:4326's latitude-first authority definition, keeps the
 * declaration and the stored coordinates in agreement for the CRS nearly every export uses. It also removes any doubt
 * for a GeoArrow consumer, the GeoArrow specification saying nothing about axis order.
 *
 * <p>Every other authority code keeps its own definition. There is no such thing as EPSG:2180 with its axes exchanged,
 * and inventing one would misrepresent the authority. Those rely instead on the Parquet rule that the stored (x, y)
 * order overrides whatever axis order the CRS declares.
 */
final class EastingFirstCrs {

    /** EPSG:4326, whose authority definition puts latitude first; OGC:CRS84 is the same CRS, longitude first. */
    private static final int WGS84 = 4326;

    private EastingFirstCrs() {}

    /** The declaration for a geometry column whose CRS resolved to {@code epsgCode}. */
    static Optional<CoordinateReferenceSystem> forEpsg(int epsgCode) {
        if (epsgCode == WGS84) {
            return Optional.of(CoordinateReferenceSystems.ogcCrs84());
        }
        return CoordinateReferenceSystems.forEpsg(epsgCode);
    }
}
