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
package io.tileverse.parquetry.geotools.data;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import org.geotools.api.referencing.operation.TransformException;
import org.geotools.data.util.ScreenMap;
import org.geotools.data.util.ScreenMapCells;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import io.tileverse.parquetry.filter.SpatialReadProbe;

/**
 * Adapts the renderer's {@link ScreenMap} to the spatial read probe. Rows are decimated as the map itself decimates a
 * feature: a sub-pixel row paints the cell holding its midpoint and is kept when that cell was unpainted. A coarse unit
 * of which only the bounds are known is skipped when it is sub-pixel and every cell touched by its box is painted. That
 * skip is exact under a world-to-screen transform of scale and translation: each row of the unit would land in one of
 * those cells and the per-row gate would drop it. Under a rotation or a reprojection a dropped row lands at most one
 * cell away from a painted one. A coarse unit proven to hold at least one geometry, every non-null geometry of which
 * satisfies the query, is, when this probe was built with a substitute shape, treated as the shapefile reader treats
 * one sub-pixel record: keyed on its midpoint cell, dropped when that cell is painted, and otherwise substituted by one
 * pixel-sized shape of the given family centred on its box, which paints the cell and is handed to the reader through
 * {@link #pollSubstitute()}. A probe built without a substitute shape answers the accepted consultation exactly as the
 * read-only one, for a read that needs attribute values not provided by a substitute.
 *
 * <p>Single-use and single-threaded, as the SPI requires: the plan that consults it and the reader that drains it run
 * on the consumer thread of one read.
 */
public final class ScreenMapReadProbe implements SpatialReadProbe {

    private static final GeometryFactory SHAPES = new GeometryFactory();

    private final ScreenMap screenMap;
    private final Optional<Class<? extends Geometry>> substituteShape;
    private final Deque<Geometry> substitutes = new ArrayDeque<>();
    private int substitutesEmitted;

    /**
     * A probe that decimates rows and skips covered units but never substitutes a unit.
     *
     * @param screenMap the renderer's map, shared with the renderer, which paints nothing of its own once it has handed
     *     the map over
     */
    public ScreenMapReadProbe(ScreenMap screenMap) {
        this.screenMap = Objects.requireNonNull(screenMap, "screenMap");
        this.substituteShape = Optional.empty();
    }

    /**
     * A probe that additionally stands in for an accepted sub-pixel unit with one pixel-sized shape.
     *
     * @param screenMap the renderer's map, shared with the renderer, which paints nothing of its own once it has handed
     *     the map over
     * @param substituteShape the geometry family of the substitutes emitted by this probe for an accepted sub-pixel
     *     unit
     */
    public ScreenMapReadProbe(ScreenMap screenMap, Class<? extends Geometry> substituteShape) {
        this.screenMap = Objects.requireNonNull(screenMap, "screenMap");
        Objects.requireNonNull(substituteShape, "substituteShape");
        this.substituteShape = Optional.of(substituteShape);
    }

    /**
     * Per-row gate: a bounding box larger than a pixel is kept; a sub-pixel box is kept only when it paints a fresh
     * pixel. A transform failure keeps the row.
     */
    @Override
    public Decision probe(double minX, double minY, double maxX, double maxY) {
        Envelope env = envelope(minX, minY, maxX, maxY);
        if (!screenMap.canSimplify(env)) {
            return Decision.keep();
        }
        try {
            boolean alreadyPainted = screenMap.checkAndSet(env);
            return alreadyPainted ? Decision.skip() : Decision.keep();
        } catch (TransformException _) {
            return Decision.keep();
        }
    }

    /**
     * Coarse consultation over a file, row group, or page. Read-only. A sub-pixel unit is skipped only when every cell
     * touched by its box is painted: each of its rows lands in one of those cells and the per-row gate would drop it.
     * That reasoning is exact under a world-to-screen transform of scale and translation; under a rotation or a
     * reprojection a dropped row lands at most one cell away from a painted one. A unit larger than a pixel, or one
     * with an unpainted touched cell, descends. Never paints. A transform failure recurses into the unit.
     */
    @Override
    public Decision probeRegion(double minX, double minY, double maxX, double maxY) {
        Envelope env = envelope(minX, minY, maxX, maxY);
        if (!screenMap.canSimplify(env)) {
            return Decision.descend();
        }
        long[] touched = touchedCells(env);
        if (touched.length == 0) {
            return Decision.descend();
        }
        for (long cell : touched) {
            if (!ScreenMapCells.isPainted(screenMap, cell)) {
                return Decision.descend();
            }
        }
        return Decision.skip();
    }

    /**
     * Consultation over a unit proven inside the query. Without a substitute shape, the read-only answer. With one, a
     * sub-pixel unit whose midpoint cell is on screen is dropped when that cell is painted, and is otherwise
     * substituted: the cell is painted now and one pixel-sized shape centred on the unit's box is queued for the
     * reader. A unit larger than a pixel, or whose midpoint cell is off screen (the map would never paint it and would
     * keep every row in it), descends.
     */
    @Override
    public Decision probeAcceptedRegion(double minX, double minY, double maxX, double maxY) {
        if (substituteShape.isEmpty()) {
            return probeRegion(minX, minY, maxX, maxY);
        }
        Envelope env = envelope(minX, minY, maxX, maxY);
        if (!screenMap.canSimplify(env)) {
            return Decision.descend();
        }
        OptionalLong midpoint = midpointCell(env);
        if (midpoint.isEmpty() || !ScreenMapCells.isOnScreen(screenMap, midpoint.getAsLong())) {
            return Decision.descend();
        }
        long cell = midpoint.getAsLong();
        if (ScreenMapCells.isPainted(screenMap, cell)) {
            return Decision.skip();
        }
        screenMap.set(ScreenMapCells.x(cell), ScreenMapCells.y(cell), true);
        substitutes.add(screenMap.getSimplifiedShape(minX, minY, maxX, maxY, SHAPES, substituteShape.orElseThrow()));
        substitutesEmitted++;
        return Decision.substitute();
    }

    /** Whether a substitute is waiting to be emitted. */
    public boolean hasSubstitute() {
        return !substitutes.isEmpty();
    }

    /** The oldest waiting substitute, removed from the queue; {@code null} when none is waiting. */
    public Geometry pollSubstitute() {
        return substitutes.poll();
    }

    /** How many units this probe substituted since it was created. */
    public int substitutesEmitted() {
        return substitutesEmitted;
    }

    private long[] touchedCells(Envelope env) {
        try {
            return ScreenMapCells.touchedCells(screenMap, env);
        } catch (TransformException _) {
            return new long[0];
        }
    }

    private OptionalLong midpointCell(Envelope env) {
        try {
            return ScreenMapCells.midpointCell(screenMap, env);
        } catch (TransformException _) {
            return OptionalLong.empty();
        }
    }

    private static Envelope envelope(double minX, double minY, double maxX, double maxY) {
        return new Envelope(minX, maxX, minY, maxY);
    }
}
