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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import org.geotools.data.util.ScreenMap;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

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

    @Test
    void coarseConsultationDescendsWhenAStraddlingBoxTouchesAnUnpaintedCell() {
        ScreenMapReadProbe probe = newProbe();

        probe.probe(10.5, 20.5, 10.5, 20.5);

        assertThat(probe.probeRegion(10.7, 20.1, 11.3, 20.5)).isEqualTo(Decision.descend());
    }

    @Test
    void coarseConsultationSkipsAStraddlingBoxWhenEveryTouchedCellIsPainted() {
        ScreenMapReadProbe probe = newProbe();
        probe.probe(10.5, 20.5, 10.5, 20.5);
        probe.probe(11.5, 20.5, 11.5, 20.5);

        assertThat(probe.probeRegion(10.7, 20.1, 11.3, 20.5))
                .as("every row of the box would land in a painted cell")
                .isEqualTo(Decision.skip());
    }

    @Test
    void anAcceptedSubPixelUnitInAnUnpaintedCellIsSubstitutedAndPaintsIt() {
        ScreenMapReadProbe probe = substitutingProbe(Polygon.class);

        Decision decision = probe.probeAcceptedRegion(10.2, 20.2, 10.4, 20.4);

        assertThat(decision).isEqualTo(Decision.substitute());
        assertThat(probe.hasSubstitute()).isTrue();
        Geometry shape = probe.pollSubstitute();
        assertThat(shape).isInstanceOf(Polygon.class);
        assertThat(shape.getEnvelopeInternal().centre().x).isCloseTo(10.3, within(1e-9));
        assertThat(shape.getEnvelopeInternal().centre().y).isCloseTo(20.3, within(1e-9));
        assertThat(shape.getEnvelopeInternal().getWidth()).as("one pixel wide").isCloseTo(1.0, within(1e-9));
        assertThat(probe.hasSubstitute()).isFalse();
        assertThat(probe.substitutesEmitted()).isEqualTo(1);
        assertThat(probe.probe(10.5, 20.5, 10.5, 20.5))
                .as("the substitute painted the cell; a later row there is dropped")
                .isEqualTo(Decision.skip());
        assertThat(probe.probeAcceptedRegion(10.6, 20.6, 10.8, 20.8))
                .as("a later accepted unit in the painted cell is skipped, not substituted")
                .isEqualTo(Decision.skip());
        assertThat(probe.substitutesEmitted()).isEqualTo(1);
    }

    @Test
    void aStraddlingAcceptedUnitIsKeyedOnItsMidpointCell() {
        ScreenMapReadProbe probe = substitutingProbe(Polygon.class);

        Decision decision = probe.probeAcceptedRegion(10.7, 20.1, 11.3, 20.5);

        assertThat(decision).isEqualTo(Decision.substitute());
        assertThat(probe.probe(11.5, 20.5, 11.5, 20.5))
                .as("the midpoint cell (11, 20) is painted")
                .isEqualTo(Decision.skip());
        assertThat(probe.probe(10.5, 20.5, 10.5, 20.5))
                .as("the other touched cell is not")
                .isEqualTo(Decision.keep());
    }

    @Test
    void aProbeWithoutASubstituteShapeNeverSubstitutes() {
        ScreenMapReadProbe probe = newProbe();

        assertThat(probe.probeAcceptedRegion(10.2, 20.2, 10.4, 20.4)).isEqualTo(Decision.descend());
        assertThat(probe.hasSubstitute()).isFalse();
        probe.probe(10.5, 20.5, 10.5, 20.5);
        assertThat(probe.probeAcceptedRegion(10.2, 20.2, 10.4, 20.4))
                .as("the read-only answer still skips a painted cell")
                .isEqualTo(Decision.skip());
    }

    @Test
    void anAcceptedUnitOffScreenOrLargerThanAPixelDescends() {
        ScreenMapReadProbe probe = substitutingProbe(Polygon.class);

        assertThat(probe.probeAcceptedRegion(300.2, 20.2, 300.4, 20.4)).isEqualTo(Decision.descend());
        assertThat(probe.probeAcceptedRegion(10, 20, 15, 25)).isEqualTo(Decision.descend());
        assertThat(probe.hasSubstitute()).isFalse();
    }

    @Test
    void theSubstituteFollowsTheShapeFamily() {
        ScreenMapReadProbe points = substitutingProbe(Point.class);
        ScreenMapReadProbe lines = substitutingProbe(LineString.class);
        ScreenMapReadProbe multiPolygons = substitutingProbe(MultiPolygon.class);

        points.probeAcceptedRegion(10.2, 20.2, 10.4, 20.4);
        lines.probeAcceptedRegion(10.2, 20.2, 10.4, 20.4);
        multiPolygons.probeAcceptedRegion(10.2, 20.2, 10.4, 20.4);

        assertThat(points.pollSubstitute()).isInstanceOf(Point.class);
        assertThat(lines.pollSubstitute()).isInstanceOf(LineString.class);
        assertThat(multiPolygons.pollSubstitute()).isInstanceOf(MultiPolygon.class);
    }

    private static ScreenMapReadProbe substitutingProbe(Class<? extends Geometry> shape) {
        return new ScreenMapReadProbe(newScreenMap(), shape);
    }

    private static ScreenMapReadProbe newProbe() {
        return new ScreenMapReadProbe(newScreenMap());
    }

    private static ScreenMap newScreenMap() {
        ScreenMap screenMap = new ScreenMap(0, 0, 256, 256);
        AffineTransform2D identity = new AffineTransform2D(1, 0, 0, 1, 0, 0);
        screenMap.setTransform(identity);
        screenMap.setSpans(1.0, 1.0);
        return screenMap;
    }
}
