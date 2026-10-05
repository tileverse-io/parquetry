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
package io.tileverse.parquetry.format;

import java.util.OptionalDouble;

import lombok.Builder;

/**
 * Geospatial bounding box; mirror of {@code BoundingBox} in {@code parquet.thrift}.
 *
 * <p>The X/Y axes are required; Z (elevation) and M (measure) extents are optional and present only when the geometry
 * carries them.
 *
 * <h2>Antimeridian wrap</h2>
 *
 * <p>Per the parquet-format Geospatial specification, when {@link #xmin()} is greater than {@link #xmax()} a box with
 * an extent ({@link #hasExtent()}) <em>wraps the antimeridian</em>: a candidate longitude {@code x} matches when
 * {@code x >= xmin || x <= xmax} rather than the usual {@code x >= xmin && x <= xmax}. Callers that aggregate bounding
 * boxes coordinate-wise (for example computing a dataset-wide extent) union their {@link #planarEnclosure()} instead; a
 * plain min/max over the raw values can land inside the gap between the two longitude ranges and lose them.
 * {@link #wrapsAntimeridian()} exposes the case explicitly.
 *
 * @param xmin minimum X coordinate (or wrap-start when {@link #wrapsAntimeridian()})
 * @param xmax maximum X coordinate (or wrap-end when {@link #wrapsAntimeridian()})
 * @param ymin minimum Y coordinate
 * @param ymax maximum Y coordinate
 * @param zmin minimum Z coordinate, when the column carries elevation
 * @param zmax maximum Z coordinate, when the column carries elevation
 * @param mmin minimum M coordinate, when the column carries measure values
 * @param mmax maximum M coordinate, when the column carries measure values
 */
@Builder
public record BoundingBox(
        double xmin,
        double xmax,
        double ymin,
        double ymax,
        OptionalDouble zmin,
        OptionalDouble zmax,
        OptionalDouble mmin,
        OptionalDouble mmax) {

    private static final double WEST_LIMIT = -180;
    private static final double EAST_LIMIT = 180;

    public BoundingBox {
        zmin = zmin == null ? OptionalDouble.empty() : zmin;
        zmax = zmax == null ? OptionalDouble.empty() : zmax;
        mmin = mmin == null ? OptionalDouble.empty() : mmin;
        mmax = mmax == null ? OptionalDouble.empty() : mmax;
    }

    /**
     * Returns {@code true} when this box has an extent and wraps the antimeridian (i.e. {@code xmin > xmax}). A box
     * without an extent, such as the inverted infinite box recorded for a chunk of empty geometries, never wraps. See
     * the class-level note on aggregation semantics.
     */
    public boolean wrapsAntimeridian() {
        return hasExtent() && xmin > xmax;
    }

    /**
     * Whether this box bounds a region: finite X and Y bounds, with {@code ymin} at or below {@code ymax}. A box
     * wrapping the antimeridian bounds one; a box with an infinite or NaN bound, or with inverted Y bounds, does not.
     * The envelope of an empty geometry is such a box.
     */
    public boolean hasExtent() {
        boolean finite =
                Double.isFinite(xmin) && Double.isFinite(xmax) && Double.isFinite(ymin) && Double.isFinite(ymax);
        return finite && ymin <= ymax;
    }

    /**
     * The planar rectangle enclosing the vertices inside this box: this box when it does not wrap the antimeridian,
     * otherwise the full longitude range {@code [-180, 180]} with this box's own y, Z and M extents. A wrapping box
     * holds longitudes in {@code [xmin, 180]} and {@code [-180, xmax]}, and a geometry with vertices in both ranges has
     * a planar envelope reaching across the gap between them. Wrapping is defined for geographic longitude in degrees
     * alone, where the full range is {@code [-180, 180]}.
     */
    public BoundingBox planarEnclosure() {
        if (!wrapsAntimeridian()) {
            return this;
        }
        return new BoundingBox(WEST_LIMIT, EAST_LIMIT, ymin, ymax, zmin, zmax, mmin, mmax);
    }

    /** Whether an X or Y bound is NaN: such a box bounds no known region and proves nothing about its geometries. */
    public boolean hasNaNBound() {
        return Double.isNaN(xmin) || Double.isNaN(xmax) || Double.isNaN(ymin) || Double.isNaN(ymax);
    }

    /**
     * Whether this box proves nothing about where its geometries lie: it neither bounds a region nor is the envelope of
     * no geometry. A NaN bound, an infinite bound out of that envelope, and inverted Y bounds make such a box; a reader
     * takes it as a missing box.
     */
    public boolean provesNothing() {
        return !hasExtent() && !isEnvelopeOfNoGeometry();
    }

    /**
     * The inverted infinite box recorded for a chunk of empty geometries: minimums at +infinity, maximums at -infinity.
     */
    private boolean isEnvelopeOfNoGeometry() {
        return xmin == Double.POSITIVE_INFINITY
                && ymin == Double.POSITIVE_INFINITY
                && xmax == Double.NEGATIVE_INFINITY
                && ymax == Double.NEGATIVE_INFINITY;
    }
}
