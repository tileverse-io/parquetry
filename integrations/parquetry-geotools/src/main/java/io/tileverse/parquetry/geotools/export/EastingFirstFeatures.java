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

import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.data.store.ReprojectingFeatureCollection;
import org.geotools.feature.FeatureCollection;
import org.geotools.referencing.CRS;

/**
 * Puts a feature collection into the coordinate order Parquet stores geometries in.
 *
 * <p>The Apache Parquet geospatial types and GeoParquet agree, in identical wording: stored coordinates are always
 * {@code (x, y)} with x the easting or longitude, and that ordering overrides the axis order the CRS declares. A
 * GeoTools collection instead holds coordinates in whatever order its CRS defines, and a WFS 2.0 GetFeature answers
 * EPSG:4326 in the authority's latitude-first order. Such a collection is reprojected to the easting-first counterpart
 * of its own CRS before its geometries are encoded.
 *
 * <p>Reprojection is left to GeoTools rather than exchanging ordinates directly: it derives the right transform for
 * every CRS, including projected ones declared northing-first, and it handles coordinate dimensionality itself.
 */
final class EastingFirstFeatures {

    private EastingFirstFeatures() {}

    /**
     * Returns {@code features} reprojected to the easting-first counterpart of its CRS, or {@code features} itself when
     * its coordinates are already easting-first.
     */
    static FeatureCollection<SimpleFeatureType, SimpleFeature> reproject(
            FeatureCollection<SimpleFeatureType, SimpleFeature> features) {
        SimpleFeatureType schema = features.getSchema();
        Optional<CoordinateReferenceSystem> eastingFirst =
                eastingFirstCounterpart(schema.getCoordinateReferenceSystem());
        if (eastingFirst.isEmpty()) {
            return features;
        }
        return new ReprojectingFeatureCollection(features, eastingFirst.get());
    }

    /**
     * Returns the easting-first counterpart of {@code crs}, or empty when there is nothing to do: {@code crs} is
     * absent, its axes already run easting-first or have no order to speak of, or it resolves to no EPSG code to look
     * the counterpart up by.
     */
    private static Optional<CoordinateReferenceSystem> eastingFirstCounterpart(CoordinateReferenceSystem crs) {
        if (crs == null || CRS.getAxisOrder(crs) != CRS.AxisOrder.NORTH_EAST) {
            return Optional.empty();
        }
        try {
            Integer code = CRS.lookupEpsgCode(crs, true);
            if (code == null) {
                return Optional.empty();
            }
            return Optional.of(CRS.decode("EPSG:" + code, true));
        } catch (FactoryException e) {
            return Optional.empty();
        }
    }
}
