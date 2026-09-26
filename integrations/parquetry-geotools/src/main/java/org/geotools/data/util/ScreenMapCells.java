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
package org.geotools.data.util;

import java.util.OptionalLong;

import org.geotools.api.referencing.operation.TransformException;
import org.locationtech.jts.geom.Envelope;

/**
 * Names the pixel cell of a {@link ScreenMap} in which a world envelope lies, as
 * {@link ScreenMap#checkAndSet(Envelope)} does: a world point goes through the map's world-to-screen transform and
 * truncates to integer screen coordinates. This class lives in the ScreenMap's own package because that transform is a
 * package-private field of the ScreenMap; nothing else of its internals is read. A cell is packed into one {@code long}
 * key.
 */
public final class ScreenMapCells {

    private ScreenMapCells() {}

    /**
     * The one cell holding every point of {@code envelope}: present only when the envelope is sub-pixel by the map's
     * spans and all four of its corners truncate to the same cell. An envelope straddling a cell boundary has no such
     * cell, even when it is narrower than a pixel. A corner sent by the transform to a non-finite screen coordinate
     * also leaves the envelope with no cell.
     *
     * <p>The four-corner test is exact for a linear world-to-screen transform: a cell is convex and the image of the
     * envelope lies in the convex hull of its four corner images, hence every point of the envelope lands in the cell
     * that its corners land in. A transform that includes a reprojection is only locally linear, and the image of a
     * sub-pixel envelope can bulge past the corner cell by far less than a pixel. Every claim made on this answer rests
     * on that premise.
     */
    public static OptionalLong cellHolding(ScreenMap map, Envelope envelope) throws TransformException {
        if (map.mt == null || !map.canSimplify(envelope)) {
            return OptionalLong.empty();
        }
        OptionalLong lowerLeft = cellOf(map, envelope.getMinX(), envelope.getMinY());
        OptionalLong upperRight = cellOf(map, envelope.getMaxX(), envelope.getMaxY());
        OptionalLong lowerRight = cellOf(map, envelope.getMaxX(), envelope.getMinY());
        OptionalLong upperLeft = cellOf(map, envelope.getMinX(), envelope.getMaxY());
        if (lowerLeft.isEmpty()) {
            return OptionalLong.empty();
        }
        boolean oneCell = lowerLeft.equals(upperRight) && lowerLeft.equals(lowerRight) && lowerLeft.equals(upperLeft);
        if (oneCell) {
            return lowerLeft;
        }
        return OptionalLong.empty();
    }

    /** Whether {@code cell} is painted. An off-screen cell reads as unpainted, as the map itself reports it. */
    public static boolean isPainted(ScreenMap map, long cell) {
        return map.get(x(cell), y(cell));
    }

    /**
     * Whether {@code cell} lies on the screen. The map keeps its origin private and reports every off-screen cell as
     * unpainted; an unpainted cell is told apart from an off-screen one by setting its bit and reading it back, and the
     * bit is cleared again before returning. A painted cell is on the screen by construction.
     */
    public static boolean isOnScreen(ScreenMap map, long cell) {
        int x = x(cell);
        int y = y(cell);
        if (map.get(x, y)) {
            return true;
        }
        map.set(x, y, true);
        boolean onScreen = map.get(x, y);
        if (onScreen) {
            map.set(x, y, false);
        }
        return onScreen;
    }

    static long pack(int x, int y) {
        return ((long) x << 32) | (y & 0xFFFFFFFFL);
    }

    static int x(long cell) {
        return (int) (cell >> 32);
    }

    static int y(long cell) {
        return (int) cell;
    }

    /**
     * The cell of one world point. A transform can send a finite world point to NaN or to infinity, and truncating such
     * a screen coordinate to an {@code int} would name an arbitrary cell; a point like that names no cell at all.
     */
    private static OptionalLong cellOf(ScreenMap map, double worldX, double worldY) throws TransformException {
        double[] point = {worldX, worldY};
        map.mt.transform(point, 0, point, 0, 1);
        if (!Double.isFinite(point[0]) || !Double.isFinite(point[1])) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(pack((int) point[0], (int) point[1]));
    }
}
