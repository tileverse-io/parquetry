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
package io.tileverse.parquetry.internal.write;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.TreeSet;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.GeospatialStatistics;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.internal.filter.spatial.WkbEnvelope;
import io.tileverse.parquetry.internal.wkb.CoordinateRun;
import io.tileverse.parquetry.internal.wkb.Dimensions;
import io.tileverse.parquetry.internal.wkb.WkbCursor;
import io.tileverse.parquetry.internal.wkb.WkbTypeCode;

import lombok.NonNull;

/**
 * Per-geometry-column accumulator that walks WKB payloads to expand a running bounding box (X, Y, optional Z, optional
 * M) and to record the set of WKB geometry type codes observed.
 *
 * <p>The wire-level walk lives in {@link WkbCursor} and {@link WkbEnvelope} drives it; this accumulator implements
 * {@link WkbEnvelope.Visitor} to expand its extents one coordinate run at a time. No intermediate byte array is
 * materialized and no JTS dependency is introduced here.
 *
 * <p>{@link #finish()} returns the assembled {@link GeospatialStatistics}: the bounding box is absent when no
 * coordinates were observed, and the geometry-type set is absent when no payload was supplied. Z and M extents are
 * tracked separately so a stream of mixed 2D / 3D / measured geometries still yields a faithful box.
 */
public final class GeospatialStatisticsAccumulator implements WkbEnvelope.Visitor {

    private boolean hasObservation;

    private double xmin = Double.POSITIVE_INFINITY;
    private double ymin = Double.POSITIVE_INFINITY;
    private double xmax = Double.NEGATIVE_INFINITY;
    private double ymax = Double.NEGATIVE_INFINITY;

    private boolean hasZ;
    private double zmin = Double.POSITIVE_INFINITY;
    private double zmax = Double.NEGATIVE_INFINITY;

    private boolean hasM;
    private double mmin = Double.POSITIVE_INFINITY;
    private double mmax = Double.NEGATIVE_INFINITY;

    private final TreeSet<Integer> geospatialTypes = new TreeSet<>();

    /**
     * Walks the WKB payload at {@code wkb}, expanding the running bounding box over every coordinate in it and
     * recording the geometry's type code. Callers retain ownership of the segment; the accumulator only reads from it.
     *
     * <p>Only the seven base geometry kinds are accepted. A geometry outside them (CIRCULARSTRING, CURVEPOLYGON, TIN,
     * TRIANGLE and the other type codes above {@link WkbTypeCode#GEOMETRYCOLLECTION}) is rejected even though it is
     * well-formed WKB: the walk cannot follow its body, and its extents would never reach the bounding box.
     *
     * @throws MalformedFileException when the payload is not valid WKB, or when its type code is not one of the seven
     *     base kinds
     */
    public void update(@NonNull MemorySegment wkb) {
        WkbEnvelope.walk(wkb, this);
    }

    @Override
    public void geometryType(int isoType) {
        geospatialTypes.add(isoType);
    }

    // S3516: the boolean is the walk-callback continue flag; this accumulator visits every run and never aborts
    @Override
    @SuppressWarnings("java:S3516")
    public boolean run(CoordinateRun run) {
        Dimensions dimensions = run.dimensions();
        int size = run.size();
        for (int i = 0; i < size; i++) {
            double x = run.x(i);
            double y = run.y(i);
            if (Double.isNaN(x) || Double.isNaN(y)) {
                continue; // POINT EMPTY and other degenerate coordinates have no real location
            }
            expandXY(x, y);
            if (dimensions.hasZ()) {
                expandZ(run.z(i));
            }
            if (dimensions.hasM()) {
                expandM(run.m(i));
            }
        }
        return true;
    }

    /**
     * Returns the assembled {@link GeospatialStatistics} for the current accumulation window. An accumulator that has
     * not been updated reports both fields absent. Calling this method does not modify the accumulator -- use
     * {@link #reset()} to start a new window.
     */
    public GeospatialStatistics finish() {
        Optional<BoundingBox> bbox = hasObservation ? Optional.of(buildBoundingBox()) : Optional.empty();
        Optional<List<Integer>> types =
                geospatialTypes.isEmpty() ? Optional.empty() : Optional.of(new ArrayList<>(geospatialTypes));
        return new GeospatialStatistics(bbox, types);
    }

    /** Clears every accumulated coordinate and type code; the accumulator behaves as freshly constructed. */
    public void reset() {
        hasObservation = false;
        xmin = Double.POSITIVE_INFINITY;
        ymin = Double.POSITIVE_INFINITY;
        xmax = Double.NEGATIVE_INFINITY;
        ymax = Double.NEGATIVE_INFINITY;
        hasZ = false;
        zmin = Double.POSITIVE_INFINITY;
        zmax = Double.NEGATIVE_INFINITY;
        hasM = false;
        mmin = Double.POSITIVE_INFINITY;
        mmax = Double.NEGATIVE_INFINITY;
        geospatialTypes.clear();
    }

    private BoundingBox buildBoundingBox() {
        return new BoundingBox(
                xmin,
                xmax,
                ymin,
                ymax,
                hasZ ? OptionalDouble.of(zmin) : OptionalDouble.empty(),
                hasZ ? OptionalDouble.of(zmax) : OptionalDouble.empty(),
                hasM ? OptionalDouble.of(mmin) : OptionalDouble.empty(),
                hasM ? OptionalDouble.of(mmax) : OptionalDouble.empty());
    }

    private void expandXY(double x, double y) {
        hasObservation = true;
        if (x < xmin) {
            xmin = x;
        }
        if (x > xmax) {
            xmax = x;
        }
        if (y < ymin) {
            ymin = y;
        }
        if (y > ymax) {
            ymax = y;
        }
    }

    private void expandZ(double z) {
        if (Double.isNaN(z)) {
            return; // a geometry can have finite XY but a missing Z; record nothing for it
        }
        hasZ = true;
        if (z < zmin) {
            zmin = z;
        }
        if (z > zmax) {
            zmax = z;
        }
    }

    private void expandM(double m) {
        if (Double.isNaN(m)) {
            return; // a geometry can have finite XY but a missing M; record nothing for it
        }
        hasM = true;
        if (m < mmin) {
            mmin = m;
        }
        if (m > mmax) {
            mmax = m;
        }
    }
}
