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
 * Geographic bounding box for {@link Predicate.BboxIntersects}.
 *
 * <p>2D variant: {@code (minX, minY, maxX, maxY)}. 3D variant adds {@code minZ, maxZ}. Coordinate semantics are the
 * consumer's responsibility; the predicate evaluator only does numeric comparisons against the column's bbox
 * statistics.
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
}
