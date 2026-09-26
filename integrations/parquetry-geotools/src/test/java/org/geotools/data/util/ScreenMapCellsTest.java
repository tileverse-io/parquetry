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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.OptionalLong;

import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;

/**
 * Pins the cells named by a ScreenMap for an envelope. The map covers pixels [0, 256) on both axes with an identity
 * transform and one world unit per pixel, as in {@code ScreenMapReadProbeTest}.
 */
class ScreenMapCellsTest {

    @Test
    void midpointCellIsTheCellCheckAndSetMarks() throws Exception {
        ScreenMap map = newMap();
        Envelope envelope = new Envelope(10.2, 10.8, 20.1, 20.9);

        OptionalLong cell = ScreenMapCells.midpointCell(map, envelope);

        assertThat(cell).isPresent();
        assertThat(ScreenMapCells.x(cell.getAsLong())).isEqualTo(10);
        assertThat(ScreenMapCells.y(cell.getAsLong())).isEqualTo(20);
        map.checkAndSet(envelope);
        assertThat(ScreenMapCells.isPainted(map, cell.getAsLong())).isTrue();
    }

    @Test
    void aStraddlingEnvelopeHasItsMidpointCellOnOneSide() throws Exception {
        OptionalLong cell = ScreenMapCells.midpointCell(newMap(), new Envelope(10.7, 11.3, 20.1, 20.5));

        assertThat(cell).isPresent();
        assertThat(ScreenMapCells.x(cell.getAsLong())).isEqualTo(11);
        assertThat(ScreenMapCells.y(cell.getAsLong())).isEqualTo(20);
    }

    @Test
    void touchedCellsNamesEveryCellUnderTheCorners() throws Exception {
        ScreenMap map = newMap();

        assertThat(ScreenMapCells.touchedCells(map, new Envelope(10.2, 10.8, 20.1, 20.9)))
                .containsExactly(ScreenMapCells.pack(10, 20));
        assertThat(ScreenMapCells.touchedCells(map, new Envelope(10.7, 11.3, 20.1, 20.5)))
                .containsExactlyInAnyOrder(ScreenMapCells.pack(10, 20), ScreenMapCells.pack(11, 20));
        assertThat(ScreenMapCells.touchedCells(map, new Envelope(10.7, 11.3, 20.7, 21.3)))
                .containsExactlyInAnyOrder(
                        ScreenMapCells.pack(10, 20),
                        ScreenMapCells.pack(11, 20),
                        ScreenMapCells.pack(10, 21),
                        ScreenMapCells.pack(11, 21));
    }

    @Test
    void aTransformThatSendsAPointOffTheNumberLineNamesNoCell() throws Exception {
        ScreenMap map = new ScreenMap(0, 0, 256, 256);
        map.setTransform(new AffineTransform2D(Double.NaN, 0, 0, 1, 0, 0));
        map.setSpans(1.0, 1.0);
        Envelope envelope = new Envelope(10.2, 10.8, 20.1, 20.9);

        assertThat(ScreenMapCells.midpointCell(map, envelope))
                .as("a non-finite screen coordinate must not truncate to cell (0, 0)")
                .isEmpty();
        assertThat(ScreenMapCells.touchedCells(map, envelope)).isEmpty();
    }

    @Test
    void paintedFollowsTheMap() {
        ScreenMap map = newMap();
        long cell = ScreenMapCells.pack(10, 20);

        assertThat(ScreenMapCells.isPainted(map, cell)).isFalse();
        map.checkAndSet(10, 20);
        assertThat(ScreenMapCells.isPainted(map, cell)).isTrue();
    }

    @Test
    void onScreenIsDecidedWithoutLeavingAMark() {
        ScreenMap map = newMap();
        long inside = ScreenMapCells.pack(10, 20);
        long outside = ScreenMapCells.pack(300, 20);

        assertThat(ScreenMapCells.isOnScreen(map, inside)).isTrue();
        assertThat(map.get(10, 20)).as("the probe restores the bit that it set").isFalse();
        assertThat(ScreenMapCells.isOnScreen(map, outside)).isFalse();

        map.checkAndSet(10, 20);

        assertThat(ScreenMapCells.isOnScreen(map, inside)).isTrue();
        assertThat(map.get(10, 20)).isTrue();
    }

    @Test
    void negativeCoordinatesRoundTripThroughTheKey() {
        long cell = ScreenMapCells.pack(-3, -7);

        assertThat(ScreenMapCells.x(cell)).isEqualTo(-3);
        assertThat(ScreenMapCells.y(cell)).isEqualTo(-7);
    }

    private static ScreenMap newMap() {
        ScreenMap screenMap = new ScreenMap(0, 0, 256, 256);
        screenMap.setTransform(new AffineTransform2D(1, 0, 0, 1, 0, 0));
        screenMap.setSpans(1.0, 1.0);
        return screenMap;
    }
}
