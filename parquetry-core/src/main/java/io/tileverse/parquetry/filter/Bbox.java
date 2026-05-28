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
package io.tileverse.parquetry.filter;

/**
 * Geographic bounding box for {@link Predicate.Spatial}.
 *
 * <p>2D form: {@code (minX, minY, maxX, maxY)}. 3D form adds {@code minZ, maxZ}. Coordinate semantics are the
 * consumer's responsibility; the predicate evaluator only does numeric comparisons against the column's bbox
 * statistics.
 *
 * @param minX minimum X coordinate
 * @param minY minimum Y coordinate
 * @param maxX maximum X coordinate
 * @param maxY maximum Y coordinate
 * @param minZ minimum Z coordinate when {@link #is3d}; ignored otherwise (set to {@code 0.0} by {@link #of2d})
 * @param maxZ maximum Z coordinate when {@link #is3d}; ignored otherwise
 * @param is3d {@code true} when the bbox is 3D and Z components are meaningful; {@code false} for the 2D form
 */
public record Bbox(double minX, double minY, double maxX, double maxY, double minZ, double maxZ, boolean is3d) {

    /** Construct a 2D bbox. */
    public static Bbox of2d(double minX, double minY, double maxX, double maxY) {
        return new Bbox(minX, minY, maxX, maxY, 0.0, 0.0, false);
    }

    /**
     * Construct a 3D bbox. Parameters follow (min, min, min, max, max, max) order: {@code (minX, minY, minZ, maxX,
     * maxY, maxZ)}.
     */
    public static Bbox of3d(double minX, double minY, double minZ, double maxX, double maxY, double maxZ) {
        return new Bbox(minX, minY, maxX, maxY, minZ, maxZ, true);
    }

    /** Test whether this bbox intersects another (both 2D, or both 3D). */
    public boolean intersects(Bbox other) {
        if (is3d != other.is3d) {
            // dimension mismatch - treat as non-intersecting
            return false;
        }
        boolean xyIntersect = !(maxX < other.minX || minX > other.maxX || maxY < other.minY || minY > other.maxY);
        if (!is3d) {
            return xyIntersect;
        }
        boolean zIntersect = !(maxZ < other.minZ || minZ > other.maxZ);
        return xyIntersect && zIntersect;
    }

    /**
     * True when this box encloses {@code other} in 2D (edges inclusive). Z is ignored; use {@link #intersects} when 3D
     * containment matters.
     */
    public boolean contains(Bbox other) {
        return minX <= other.minX && minY <= other.minY && maxX >= other.maxX && maxY >= other.maxY;
    }

    /** True when {@code other} encloses this box in 2D - the inverse of {@link #contains}. */
    public boolean coveredBy(Bbox other) {
        return other.contains(this);
    }

    /** True when the four 2D edges are equal; Z is ignored. */
    public boolean sameBox2d(Bbox other) {
        return minX == other.minX && minY == other.minY && maxX == other.maxX && maxY == other.maxY;
    }
}
