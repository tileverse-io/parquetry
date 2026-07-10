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

import java.util.Objects;

import org.geotools.api.referencing.operation.TransformException;
import org.geotools.data.util.ScreenMap;
import org.locationtech.jts.geom.Envelope;

import io.tileverse.parquetry.filter.SpatialReadProbe;

/**
 * Adapts a GeoTools {@link ScreenMap} to the {@link SpatialReadProbe} consulted during a spatial read. The ScreenMap is
 * a packed one-bit-per-pixel image of the screen; this probe uses it to drop features that fall into a pixel an earlier
 * feature already painted, which keeps overview-zoom renders cheap over dense data.
 *
 * <p>The two consultations honor the probe's read-only-versus-painting split. The per-row {@link #probe} is the only
 * one allowed to paint: it marks a sub-pixel cell occupied through {@link ScreenMap#checkAndSet(Envelope)}. The coarse
 * {@link #probeRegion} is read-only: it inspects the paint state through {@link ScreenMap#get(Envelope)} and never
 * mutates it, because a coarse unit's individual rows have not been emitted yet and painting their shared cell would
 * drop every one of them and leave the cell empty.
 *
 * <p>The wrapped ScreenMap is mutable paint state. This probe is single-use and single-threaded and must not be shared
 * across concurrent reads.
 */
public final class ScreenMapReadProbe implements SpatialReadProbe {

    private final ScreenMap screenMap;

    public ScreenMapReadProbe(ScreenMap screenMap) {
        this.screenMap = Objects.requireNonNull(screenMap, "screenMap");
    }

    /**
     * Per-row consultation. A bounding box bigger than a pixel is decoded and drawn in full. A sub-pixel bounding box
     * is dropped when its cell is already painted; otherwise this paints the cell and keeps the row. A transform
     * failure falls back to decoding the row.
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
        } catch (TransformException e) {
            return Decision.keep();
        }
    }

    /**
     * Coarse consultation over a file, row group, or page. Read-only: a unit bigger than a pixel spans many cells and
     * cannot be pre-skipped, and a sub-pixel unit is skipped only when its cell is already painted by earlier rows.
     * Never paints. A transform failure recurses into the unit.
     */
    @Override
    public Decision probeRegion(double minX, double minY, double maxX, double maxY) {
        Envelope env = envelope(minX, minY, maxX, maxY);
        if (!screenMap.canSimplify(env)) {
            return Decision.descend();
        }
        try {
            boolean alreadyPainted = screenMap.get(env);
            return alreadyPainted ? Decision.skip() : Decision.descend();
        } catch (TransformException e) {
            return Decision.descend();
        }
    }

    private static Envelope envelope(double minX, double minY, double maxX, double maxY) {
        return new Envelope(minX, maxX, minY, maxY);
    }
}
