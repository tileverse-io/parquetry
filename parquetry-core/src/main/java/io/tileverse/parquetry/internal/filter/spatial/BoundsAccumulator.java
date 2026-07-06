/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.internal.filter.spatial;

import java.util.Optional;
import java.util.OptionalDouble;

import io.tileverse.parquetry.format.BoundingBox;

/**
 * Running union of bounding boxes that answers 2D containment queries.
 *
 * <p>The accumulated box is always a conservative enclosure of every contribution: equal to or larger than the true
 * extent, never smaller. The 2D extent is exact. The Z and M extents are best-effort and present only when every
 * contribution was a metadata box that provided them; the first contribution without a dimension - a Z-less box or any
 * {@link #unionXy} scan fold, which is 2D only - drops that dimension permanently, because an extent that ignores a
 * dimension some contributions lack could not enclose them in that dimension. A dropped dimension never returns, even
 * if later contributions provide it. Under concurrent fan-outs with containment skips, which Z and M contributions are
 * seen can depend on completion order; the 2D extent never does.
 *
 * <p>Callers apply two trust levels. A conservative metadata box may only justify skipping work, through
 * {@link #covers(BoundingBox)}: containment of an enclosure implies containment of whatever it encloses. A box may be
 * added to the extent without scanning only when it is the tight box of a unit whose rows all match the query.
 *
 * <p>All methods are synchronized because the dataset fan-out shares one instance across the producer threads that
 * expand it. The extent only ever grows. Because of that, a {@link #covers(BoundingBox)} caller that reads a stale
 * (smaller) extent can only under-report coverage: it returns {@code false} where the current extent would return
 * {@code true}, which costs an extra visit but never wrongly reports a box as covered and never skips real data.
 */
public final class BoundsAccumulator {

    private boolean empty = true;
    private double minX;
    private double minY;
    private double maxX;
    private double maxY;

    private boolean hasZ;
    private double zMin;
    private double zMax;
    private boolean zPoisoned;

    private boolean hasM;
    private double mMin;
    private double mMax;
    private boolean mPoisoned;

    /** Expands the extent by the given box, combining Z and M only while every contribution keeps providing them. */
    public synchronized void union(BoundingBox box) {
        combineZ(box);
        combineM(box);
        expandXy(box.xmin(), box.ymin(), box.xmax(), box.ymax());
    }

    /**
     * Expands the 2D extent by the given rectangle. This is the scan-fold entry; a scanned rectangle is 2D only, and
     * one fold drops Z and M for good, exactly like a union with a box that lacks them.
     */
    public synchronized void unionXy(double xLow, double yLow, double xHigh, double yHigh) {
        dropZ();
        dropM();
        expandXy(xLow, yLow, xHigh, yHigh);
    }

    /**
     * Returns {@code true} when the box fits inside the accumulated 2D extent with inclusive edges; false while empty.
     */
    public synchronized boolean covers(BoundingBox box) {
        if (empty) {
            return false;
        }
        return box.xmin() >= minX && box.xmax() <= maxX && box.ymin() >= minY && box.ymax() <= maxY;
    }

    /** Returns the accumulated box, or empty until the first union. */
    public synchronized Optional<BoundingBox> snapshot() {
        if (empty) {
            return Optional.empty();
        }
        return Optional.of(new BoundingBox(minX, maxX, minY, maxY, zLow(), zHigh(), mLow(), mHigh()));
    }

    private void expandXy(double xLow, double yLow, double xHigh, double yHigh) {
        if (empty) {
            minX = xLow;
            minY = yLow;
            maxX = xHigh;
            maxY = yHigh;
            empty = false;
            return;
        }
        minX = Math.min(minX, xLow);
        minY = Math.min(minY, yLow);
        maxX = Math.max(maxX, xHigh);
        maxY = Math.max(maxY, yHigh);
    }

    private void combineZ(BoundingBox box) {
        if (box.zmin().isPresent() && box.zmax().isPresent()) {
            unionZ(box.zmin().getAsDouble(), box.zmax().getAsDouble());
        } else {
            dropZ();
        }
    }

    private void dropZ() {
        zPoisoned = true;
        hasZ = false;
    }

    private void unionZ(double zLow, double zHigh) {
        if (zPoisoned) {
            return;
        }
        if (hasZ) {
            zMin = Math.min(zMin, zLow);
            zMax = Math.max(zMax, zHigh);
            return;
        }
        zMin = zLow;
        zMax = zHigh;
        hasZ = true;
    }

    private void combineM(BoundingBox box) {
        if (box.mmin().isPresent() && box.mmax().isPresent()) {
            unionM(box.mmin().getAsDouble(), box.mmax().getAsDouble());
        } else {
            dropM();
        }
    }

    private void dropM() {
        mPoisoned = true;
        hasM = false;
    }

    private void unionM(double mLow, double mHigh) {
        if (mPoisoned) {
            return;
        }
        if (hasM) {
            mMin = Math.min(mMin, mLow);
            mMax = Math.max(mMax, mHigh);
            return;
        }
        mMin = mLow;
        mMax = mHigh;
        hasM = true;
    }

    private OptionalDouble zLow() {
        return hasZ ? OptionalDouble.of(zMin) : OptionalDouble.empty();
    }

    private OptionalDouble zHigh() {
        return hasZ ? OptionalDouble.of(zMax) : OptionalDouble.empty();
    }

    private OptionalDouble mLow() {
        return hasM ? OptionalDouble.of(mMin) : OptionalDouble.empty();
    }

    private OptionalDouble mHigh() {
        return hasM ? OptionalDouble.of(mMax) : OptionalDouble.empty();
    }
}
