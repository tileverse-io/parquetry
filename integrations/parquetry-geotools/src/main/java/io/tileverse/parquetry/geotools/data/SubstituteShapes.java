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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

/**
 * The geometry family of a substitute feature, chosen from the GeoParquet {@code geometry_types} declared for the
 * primary column to let the renderer's symbolizers see the family for which the style was written. A single declared
 * type is its own family; a family declared in both single and multi form takes the single type; a mix of families, an
 * unknown type or no declaration at all falls back to a polygon, which every symbolizer of an area layer can paint. A
 * dimension suffix ({@code Z}, {@code M}, {@code ZM}) plays no part.
 */
final class SubstituteShapes {

    private SubstituteShapes() {}

    static Class<? extends Geometry> familyOf(List<String> geometryTypes) {
        Set<String> names = new HashSet<>();
        for (String type : geometryTypes) {
            names.add(baseName(type));
        }
        if (names.isEmpty()) {
            return Polygon.class;
        }
        if (names.contains("Point") && withinFamily(names, "Point", "MultiPoint")) {
            return Point.class;
        }
        if (names.equals(Set.of("MultiPoint"))) {
            return MultiPoint.class;
        }
        if (names.contains("LineString") && withinFamily(names, "LineString", "MultiLineString")) {
            return LineString.class;
        }
        if (names.equals(Set.of("MultiLineString"))) {
            return MultiLineString.class;
        }
        if (names.equals(Set.of("MultiPolygon"))) {
            return MultiPolygon.class;
        }
        return Polygon.class;
    }

    /** Whether every declared name is one of the family's two spellings. */
    private static boolean withinFamily(Set<String> names, String single, String multi) {
        for (String name : names) {
            if (!name.equals(single) && !name.equals(multi)) {
                return false;
            }
        }
        return true;
    }

    /** The type name without its dimension suffix: {@code "Polygon Z"} reads as {@code "Polygon"}. */
    private static String baseName(String type) {
        int space = type.indexOf(' ');
        return space < 0 ? type : type.substring(0, space);
    }
}
