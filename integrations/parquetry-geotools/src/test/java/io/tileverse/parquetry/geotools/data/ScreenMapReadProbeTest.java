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
package io.tileverse.parquetry.geotools.data;

import static org.assertj.core.api.Assertions.assertThat;

import org.geotools.data.util.ScreenMap;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.SpatialReadProbe.Decision;

/**
 * Pins how {@link ScreenMapReadProbe} adapts a GeoTools {@link ScreenMap} to the spatial read probe. Each scenario
 * builds a fresh ScreenMap and probe to control the paint state.
 *
 * <p>The ScreenMap is set up over a 256x256 pixel area with an identity world-to-screen transform and one world unit
 * per pixel ({@code setSpans(1.0, 1.0)}). World coordinates therefore map 1:1 to pixels: a bounding box narrower and
 * shorter than one world unit collapses into a single pixel (sub-pixel), and a bounding box of several world units
 * spans many pixels.
 */
class ScreenMapReadProbeTest {

    @Test
    void biggerThanAPixelIsAlwaysKeptAndNeverPreSkipped() {
        ScreenMapReadProbe probe = newProbe();

        Decision leaf = probe.probe(10, 10, 15, 15);
        Decision region = probe.probeRegion(10, 10, 15, 15);

        assertThat(leaf).isEqualTo(Decision.keep());
        assertThat(region).isEqualTo(Decision.descend());
    }

    @Test
    void firstFeatureInACellIsKeptAndPaintsItThenTheNextIsSkipped() {
        ScreenMapReadProbe probe = newProbe();

        Decision first = probe.probe(10.5, 10.5, 10.5, 10.5);
        Decision second = probe.probe(10.5, 10.5, 10.5, 10.5);

        assertThat(first).isEqualTo(Decision.keep());
        assertThat(second).isEqualTo(Decision.skip());
    }

    @Test
    void coarseConsultationReadsTheCellWithoutPaintingIt() {
        ScreenMapReadProbe probe = newProbe();

        Decision region = probe.probeRegion(10.5, 10.5, 10.5, 10.5);
        Decision leafAfterRegion = probe.probe(10.5, 10.5, 10.5, 10.5);

        assertThat(region).as("an unpainted cell cannot be pre-skipped").isEqualTo(Decision.descend());
        assertThat(leafAfterRegion)
                .as("probeRegion must not paint; the leaf still sees a fresh cell")
                .isEqualTo(Decision.keep());
    }

    @Test
    void coarseConsultationSkipsACellAlreadyPaintedByTheLeaf() {
        ScreenMapReadProbe probe = newProbe();

        Decision leaf = probe.probe(10.5, 10.5, 10.5, 10.5);
        Decision region = probe.probeRegion(10.5, 10.5, 10.5, 10.5);

        assertThat(leaf).isEqualTo(Decision.keep());
        assertThat(region).isEqualTo(Decision.skip());
    }

    private static ScreenMapReadProbe newProbe() {
        ScreenMap screenMap = new ScreenMap(0, 0, 256, 256);
        AffineTransform2D identity = new AffineTransform2D(1, 0, 0, 1, 0, 0);
        screenMap.setTransform(identity);
        screenMap.setSpans(1.0, 1.0);
        return new ScreenMapReadProbe(screenMap);
    }
}
