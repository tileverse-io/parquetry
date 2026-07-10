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
 * <p>Per the parquet-format Geospatial specification, when {@link #xmin()} is greater than {@link #xmax()} the box
 * <em>wraps the antimeridian</em>: a candidate longitude {@code x} matches when {@code x >= xmin || x <= xmax} rather
 * than the usual {@code x >= xmin && x <= xmax}. Callers that aggregate bounding boxes (for example computing a
 * dataset-wide extent) must split a wrapping box into two non-wrapping pieces before union; otherwise the union
 * collapses to a globe-spanning extent. {@link #wrapsAntimeridian()} exposes the case explicitly.
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

    public BoundingBox {
        zmin = zmin == null ? OptionalDouble.empty() : zmin;
        zmax = zmax == null ? OptionalDouble.empty() : zmax;
        mmin = mmin == null ? OptionalDouble.empty() : mmin;
        mmax = mmax == null ? OptionalDouble.empty() : mmax;
    }

    /**
     * Returns {@code true} when this box wraps the antimeridian (i.e. {@code xmin > xmax}). See the class-level note on
     * aggregation semantics.
     */
    public boolean wrapsAntimeridian() {
        return xmin > xmax;
    }
}
