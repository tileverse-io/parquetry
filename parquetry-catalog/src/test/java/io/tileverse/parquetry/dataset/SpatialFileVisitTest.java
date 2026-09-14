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
package io.tileverse.parquetry.dataset;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.function.IntFunction;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.SpatialReadProbe;
import io.tileverse.parquetry.filter.SpatialReadProbe.Decision;
import io.tileverse.parquetry.format.BoundingBox;

class SpatialFileVisitTest {

    /** Keeps every row and descends into every region: a probe that never skips a file. */
    private static final SpatialReadProbe NEVER_SKIPS = (minX, minY, maxX, maxY) -> Decision.keep();

    /** Reports every region already covered: only a guard inside the plan can keep a file. */
    private static final SpatialReadProbe SKIPS_EVERY_REGION = new SpatialReadProbe() {
        @Override
        public Decision probe(double minX, double minY, double maxX, double maxY) {
            return Decision.keep();
        }

        @Override
        public Decision probeRegion(double minX, double minY, double maxX, double maxY) {
            return Decision.skip();
        }
    };

    @Test
    void ordersSurvivorsAscendingByTheirBoxesMinimumCorner() {
        IntFunction<Optional<BoundingBox>> boxes = boxesOf(Map.of(
                0, box2d(5, 5, 6, 6),
                1, box2d(0, 3, 1, 4),
                2, box2d(0, 0, 1, 1)));

        SpatialFileVisit visit = SpatialFileVisit.plan(List.of(0, 1, 2), boxes, NEVER_SKIPS);

        assertThat(visit.order()).containsExactly(2, 1, 0);
    }

    @Test
    void boxLessFilesSortLastKeepingTheirRelativeOrder() {
        IntFunction<Optional<BoundingBox>> boxes = boxesOf(Map.of(1, box2d(1, 1, 2, 2)));

        SpatialFileVisit visit = SpatialFileVisit.plan(List.of(0, 1, 2, 3), boxes, NEVER_SKIPS);

        assertThat(visit.order()).containsExactly(1, 0, 2, 3);
    }

    @Test
    void planningNoSurvivorsYieldsAnEmptyOrder() {
        SpatialFileVisit visit = SpatialFileVisit.plan(List.of(), index -> Optional.empty(), NEVER_SKIPS);

        assertThat(visit.order()).isEmpty();
    }

    @Test
    void skipsAFileWhoseBoxTheProbeReportsCovered() {
        IntFunction<Optional<BoundingBox>> boxes = boxesOf(Map.of(0, box2d(0, 0, 1, 1), 1, box2d(7, 7, 8, 8)));
        SpatialReadProbe coversSeven = new SpatialReadProbe() {
            @Override
            public Decision probe(double minX, double minY, double maxX, double maxY) {
                return Decision.keep();
            }

            @Override
            public Decision probeRegion(double minX, double minY, double maxX, double maxY) {
                return minX == 7 ? Decision.skip() : Decision.descend();
            }
        };

        SpatialFileVisit visit = SpatialFileVisit.plan(List.of(0, 1), boxes, coversSeven);

        assertThat(visit.skips(1)).isTrue();
        assertThat(visit.skips(0)).isFalse();
    }

    @Test
    void neverSkipsABoxLessFile() {
        SpatialFileVisit visit = SpatialFileVisit.plan(List.of(0), index -> Optional.empty(), SKIPS_EVERY_REGION);

        assertThat(visit.skips(0)).isFalse();
    }

    @Test
    void neverSkipsAFileWhoseBoxWrapsTheAntimeridian() {
        BoundingBox wrapping = box2d(170, 0, -170, 10);
        assertThat(wrapping.wrapsAntimeridian()).isTrue();
        IntFunction<Optional<BoundingBox>> boxes = boxesOf(Map.of(0, wrapping));

        SpatialFileVisit visit = SpatialFileVisit.plan(List.of(0), boxes, SKIPS_EVERY_REGION);

        assertThat(visit.skips(0)).isFalse();
        assertThat(visit.order()).containsExactly(0);
    }

    @Test
    void consultsTheProbeWithTheFilesOwnBox() {
        List<double[]> consulted = new ArrayList<>();
        SpatialReadProbe recording = new SpatialReadProbe() {
            @Override
            public Decision probe(double minX, double minY, double maxX, double maxY) {
                return Decision.keep();
            }

            @Override
            public Decision probeRegion(double minX, double minY, double maxX, double maxY) {
                consulted.add(new double[] {minX, minY, maxX, maxY});
                return Decision.descend();
            }
        };
        IntFunction<Optional<BoundingBox>> boxes = boxesOf(Map.of(3, box2d(-1, -2, 3, 4)));

        SpatialFileVisit visit = SpatialFileVisit.plan(List.of(3), boxes, recording);
        visit.skips(3);

        assertThat(consulted).hasSize(1);
        assertThat(consulted.get(0)).containsExactly(-1, -2, 3, 4);
    }

    private static IntFunction<Optional<BoundingBox>> boxesOf(Map<Integer, BoundingBox> boxes) {
        return index -> Optional.ofNullable(boxes.get(index));
    }

    private static BoundingBox box2d(double minX, double minY, double maxX, double maxY) {
        return new BoundingBox(
                minX,
                maxX,
                minY,
                maxY,
                OptionalDouble.empty(),
                OptionalDouble.empty(),
                OptionalDouble.empty(),
                OptionalDouble.empty());
    }
}
