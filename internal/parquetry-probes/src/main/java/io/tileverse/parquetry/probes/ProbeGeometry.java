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
package io.tileverse.parquetry.probes;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

/** The query diamond shared by the spatial scenarios: the probe builds it for the JVM engines, DuckDB needs its WKT. */
final class ProbeGeometry {

    private ProbeGeometry() {}

    /** A diamond (rotated square) centred at {@code cx,cy} with the given half-diagonal. */
    static Geometry diamond(double cx, double cy, double r) {
        GeometryFactory factory = new GeometryFactory();
        Coordinate top = new Coordinate(cx, cy + r);
        Coordinate right = new Coordinate(cx + r, cy);
        Coordinate bottom = new Coordinate(cx, cy - r);
        Coordinate left = new Coordinate(cx - r, cy);
        Coordinate[] ring = {top, right, bottom, left, top};
        return factory.createPolygon(ring);
    }

    static String wkt(Geometry geometry) {
        Coordinate[] ring = geometry.getCoordinates();
        StringBuilder wkt = new StringBuilder("POLYGON((");
        for (int i = 0; i < ring.length; i++) {
            if (i > 0) {
                wkt.append(", ");
            }
            wkt.append(ring[i].x).append(' ').append(ring[i].y);
        }
        return wkt.append("))").toString();
    }
}
