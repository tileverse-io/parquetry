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
import java.util.stream.LongStream;

import org.geotools.api.referencing.operation.TransformException;
import org.locationtech.jts.geom.Envelope;

/**
 * Names the pixel cells of a {@link ScreenMap} touched by a world envelope, as {@link ScreenMap#checkAndSet(Envelope)}
 * names the cell marked by it: a world point goes through the map's world-to-screen transform and truncates to integer
 * screen coordinates. This class lives in the ScreenMap's own package because that transform is a package-private field
 * of the ScreenMap; nothing else of its internals is read. A cell is packed into one {@code long} key.
 */
public final class ScreenMapCells {

    private static final long[] NO_CELLS = new long[0];

    private ScreenMapCells() {}

    /**
     * The cell that {@link ScreenMap#checkAndSet(Envelope)} marks for {@code envelope}: the one holding its midpoint.
     * Empty when the map has no transform, or when the transform sends the midpoint to a non-finite screen coordinate.
     */
    public static OptionalLong midpointCell(ScreenMap map, Envelope envelope) throws TransformException {
        if (map.mt == null) {
            return OptionalLong.empty();
        }
        double midX = (envelope.getMinX() + envelope.getMaxX()) / 2;
        double midY = (envelope.getMinY() + envelope.getMaxY()) / 2;
        return cellOf(map, midX, midY);
    }

    /**
     * The distinct cells named by the four corners of {@code envelope}, at most four. The caller passes an envelope
     * narrower than one pixel on both axes, as {@link ScreenMap#canSimplify(Envelope)} reports it; the coverage bound
     * below holds under that precondition alone.
     *
     * <p>Take a world-to-screen transform built from a scale and a translation, the renderer's usual case. The screen
     * image of a sub-pixel envelope is then an axis-aligned rectangle shorter and narrower than one pixel, hence it
     * spans at most two cell columns and at most two cell rows. Those columns and rows are the ones named by its
     * corners, and the four corner cells are exactly the cells overlapped by the image: every point of the envelope
     * lands in one of them. A transform with a rotation, or one with a reprojection, is only locally of that form, and
     * the image of a sub-pixel envelope can then reach one cell outside the four, always adjacent to them.
     *
     * <p>Empty when the map has no transform, or when a corner lands on a non-finite screen coordinate.
     */
    public static long[] touchedCells(ScreenMap map, Envelope envelope) throws TransformException {
        if (map.mt == null) {
            return NO_CELLS;
        }
        OptionalLong lowerLeft = cellOf(map, envelope.getMinX(), envelope.getMinY());
        OptionalLong lowerRight = cellOf(map, envelope.getMaxX(), envelope.getMinY());
        OptionalLong upperLeft = cellOf(map, envelope.getMinX(), envelope.getMaxY());
        OptionalLong upperRight = cellOf(map, envelope.getMaxX(), envelope.getMaxY());
        if (lowerLeft.isEmpty() || lowerRight.isEmpty() || upperLeft.isEmpty() || upperRight.isEmpty()) {
            return NO_CELLS;
        }
        return LongStream.of(
                        lowerLeft.getAsLong(), lowerRight.getAsLong(), upperLeft.getAsLong(), upperRight.getAsLong())
                .distinct()
                .toArray();
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

    public static int x(long cell) {
        return (int) (cell >> 32);
    }

    public static int y(long cell) {
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
