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
package io.tileverse.parquetry.testsupport;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import io.tileverse.parquetry.filter.SpatialReadProbe;

/**
 * A scripted {@link SpatialReadProbe} over a square grid of cells whose side is a fixed span. A box is sub-pixel when
 * it is shorter than the span on both axes, and it is keyed on the cell holding its midpoint, as a GeoTools ScreenMap
 * keys an envelope. A row paints its cell and is kept when the cell was unpainted; a read-only region is skipped when
 * every cell named by its four corners is painted and descends otherwise; an accepted sub-pixel region paints its
 * midpoint cell and is substituted when that cell was unpainted, and is skipped when it was painted. The painted cells
 * are open to assertions.
 *
 * <p>This mirrors what a GeoTools ScreenMap does at a chosen pixel size, without a rendering stack: a differential test
 * drives one grid through the substituting contract and an identical grid through {@link #withoutSubstitution}, then
 * checks that each grid painted within one cell of the other and that each painted cell of the substituting grid
 * yielded one row or one substitute.
 */
public final class SpanCellProbe implements SpatialReadProbe {

    private final double span;
    private final Set<Cell> painted = new HashSet<>();
    private int substitutes;

    public SpanCellProbe(double span) {
        if (span <= 0) {
            throw new IllegalArgumentException("span must be positive: " + span);
        }
        this.span = span;
    }

    /**
     * A view of {@code delegate} that answers the per-row and the read-only coarse consultation through it and leaves
     * {@link SpatialReadProbe#probeAcceptedRegion} at its interface default. A read through the view never substitutes,
     * which makes it the reference side of a differential comparison.
     */
    public static SpatialReadProbe withoutSubstitution(SpanCellProbe delegate) {
        return new ReadOnlyView(Objects.requireNonNull(delegate, "delegate"));
    }

    @Override
    public Decision probe(double minX, double minY, double maxX, double maxY) {
        if (!subPixel(minX, minY, maxX, maxY)) {
            return Decision.keep();
        }
        return painted.add(midpointCell(minX, minY, maxX, maxY)) ? Decision.keep() : Decision.skip();
    }

    @Override
    public Decision probeRegion(double minX, double minY, double maxX, double maxY) {
        if (!subPixel(minX, minY, maxX, maxY)) {
            return Decision.descend();
        }
        for (Cell touched : touchedCells(minX, minY, maxX, maxY)) {
            if (!painted.contains(touched)) {
                return Decision.descend();
            }
        }
        return Decision.skip();
    }

    @Override
    public Decision probeAcceptedRegion(double minX, double minY, double maxX, double maxY) {
        if (!subPixel(minX, minY, maxX, maxY)) {
            return Decision.descend();
        }
        if (!painted.add(midpointCell(minX, minY, maxX, maxY))) {
            return Decision.skip();
        }
        substitutes++;
        return Decision.substitute();
    }

    /** How many units this probe substituted since it was created. */
    public int substitutes() {
        return substitutes;
    }

    /** The cells painted by rows and substitutes so far. */
    public Set<Cell> painted() {
        return painted;
    }

    /** One cell of the grid, in cell coordinates. */
    public record Cell(int x, int y) {}

    private boolean subPixel(double minX, double minY, double maxX, double maxY) {
        return maxX - minX < span && maxY - minY < span;
    }

    private Cell midpointCell(double minX, double minY, double maxX, double maxY) {
        return cellOf((minX + maxX) / 2, (minY + maxY) / 2);
    }

    private Set<Cell> touchedCells(double minX, double minY, double maxX, double maxY) {
        Set<Cell> cells = new HashSet<>(4);
        cells.add(cellOf(minX, minY));
        cells.add(cellOf(maxX, minY));
        cells.add(cellOf(minX, maxY));
        cells.add(cellOf(maxX, maxY));
        return cells;
    }

    private Cell cellOf(double x, double y) {
        return new Cell(index(x), index(y));
    }

    private int index(double coordinate) {
        return (int) Math.floor(coordinate / span);
    }

    /** Delegates the two consultations of the base contract and inherits the accepted-region default. */
    private record ReadOnlyView(SpanCellProbe delegate) implements SpatialReadProbe {

        @Override
        public Decision probe(double minX, double minY, double maxX, double maxY) {
            return delegate.probe(minX, minY, maxX, maxY);
        }

        @Override
        public Decision probeRegion(double minX, double minY, double maxX, double maxY) {
            return delegate.probeRegion(minX, minY, maxX, maxY);
        }
    }
}
