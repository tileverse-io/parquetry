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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.tileverse.parquetry.filter.SpatialReadProbe;

/**
 * A scripted {@link SpatialReadProbe} whose screen cells are integer spans of the X axis. A row paints the cell holding
 * its point; a region is sub-pixel when its X span is shorter than one cell, and it is keyed on the cell holding its
 * midpoint. A read-only region is skipped when every cell touched by its span is painted; an accepted sub-pixel region
 * in an unpainted midpoint cell paints that cell and is substituted, in a painted one it is skipped; a wider region
 * descends. Every coarse consultation is appended to {@link #log()}, and the painted cells are open to assertions.
 */
public final class CellProbe implements SpatialReadProbe {

    private final Set<Integer> painted = new HashSet<>();
    private final List<String> log = new ArrayList<>();
    private int substitutes;

    @Override
    public Decision probe(double minX, double minY, double maxX, double maxY) {
        return painted.add(cell(midpoint(minX, maxX))) ? Decision.keep() : Decision.skip();
    }

    @Override
    public Decision probeRegion(double minX, double minY, double maxX, double maxY) {
        log.add("region " + minX + ".." + maxX);
        if (!subPixel(minX, maxX)) {
            return Decision.descend();
        }
        boolean everyTouchedCellPainted = painted.contains(cell(minX)) && painted.contains(cell(maxX));
        return everyTouchedCellPainted ? Decision.skip() : Decision.descend();
    }

    @Override
    public Decision probeAcceptedRegion(double minX, double minY, double maxX, double maxY) {
        log.add("accepted " + minX + ".." + maxX);
        if (!subPixel(minX, maxX)) {
            return Decision.descend();
        }
        if (!painted.add(cell(midpoint(minX, maxX)))) {
            return Decision.skip();
        }
        substitutes++;
        return Decision.substitute();
    }

    /** The cells painted by rows and substitutes, open for a test to pre-paint one. */
    public Set<Integer> painted() {
        return painted;
    }

    /** One entry per region or accepted-region consultation, in the order in which they were answered. */
    public List<String> log() {
        return log;
    }

    /** How many units this probe substituted since it was created. */
    public int substitutes() {
        return substitutes;
    }

    public static boolean subPixel(double minX, double maxX) {
        return maxX - minX < 1.0;
    }

    public static int cell(double x) {
        return (int) Math.floor(x);
    }

    private static double midpoint(double minX, double maxX) {
        return (minX + maxX) / 2;
    }
}
