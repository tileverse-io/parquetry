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
package io.tileverse.parquetry.internal.filter.spatial;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.OptionalDouble;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.BoundingBox;

class BoundsAccumulatorTest {

    @Test
    void startsEmptyAndCoversNothing() {
        BoundsAccumulator accumulator = new BoundsAccumulator();

        assertThat(accumulator.snapshot()).isEmpty();
        assertThat(accumulator.covers(box(0, 0, 10, 10))).isFalse();
    }

    @Test
    void unionExpandsAndSnapshots() {
        BoundsAccumulator accumulator = new BoundsAccumulator();

        accumulator.union(box(-10, -10, -5, -5));
        accumulator.union(box(5, 5, 10, 10));

        Optional<BoundingBox> snapshot = accumulator.snapshot();
        assertThat(snapshot).isPresent();
        BoundingBox enclosing = snapshot.orElseThrow();
        assertThat(enclosing.xmin()).isEqualTo(-10);
        assertThat(enclosing.ymin()).isEqualTo(-10);
        assertThat(enclosing.xmax()).isEqualTo(10);
        assertThat(enclosing.ymax()).isEqualTo(10);
    }

    @Test
    void coversIsInclusiveContainment() {
        BoundsAccumulator accumulator = new BoundsAccumulator();
        accumulator.union(box(0, 0, 10, 10));

        assertThat(accumulator.covers(box(0, 0, 10, 10))).isTrue();
        assertThat(accumulator.covers(box(2, 2, 8, 8))).isTrue();
        assertThat(accumulator.covers(box(0, 0, 10, 11))).isFalse();
    }

    @Test
    void zSurvivesOnlyWhenEveryContributionHasIt() {
        BoundsAccumulator accumulator = new BoundsAccumulator();

        accumulator.union(boxZ(0, 0, 10, 10, 1, 5));
        accumulator.union(boxZ(5, 5, 20, 20, 3, 8));
        BoundingBox combined = accumulator.snapshot().orElseThrow();
        assertThat(combined.zmin()).hasValue(1);
        assertThat(combined.zmax()).hasValue(8);

        accumulator.union(box(-5, -5, 0, 0));
        assertThat(accumulator.snapshot().orElseThrow().zmin()).isEmpty();
        assertThat(accumulator.snapshot().orElseThrow().zmax()).isEmpty();

        accumulator.union(boxZ(0, 0, 1, 1, 2, 9));
        assertThat(accumulator.snapshot().orElseThrow().zmin()).isEmpty();
        assertThat(accumulator.snapshot().orElseThrow().zmax()).isEmpty();
    }

    @Test
    void unionXyDropsZAndMForGood() {
        BoundsAccumulator accumulator = new BoundsAccumulator();

        accumulator.union(boxZm(0, 0, 10, 10, 1, 5, 100, 200));
        BoundingBox beforeFold = accumulator.snapshot().orElseThrow();
        assertThat(beforeFold.zmin()).hasValue(1);
        assertThat(beforeFold.zmax()).hasValue(5);
        assertThat(beforeFold.mmin()).hasValue(100);
        assertThat(beforeFold.mmax()).hasValue(200);

        accumulator.unionXy(20, 20, 30, 30);
        BoundingBox afterFold = accumulator.snapshot().orElseThrow();
        assertThat(afterFold.zmin()).isEmpty();
        assertThat(afterFold.zmax()).isEmpty();
        assertThat(afterFold.mmin()).isEmpty();
        assertThat(afterFold.mmax()).isEmpty();

        accumulator.union(boxZm(0, 0, 1, 1, 2, 9, 50, 60));
        BoundingBox afterLaterUnion = accumulator.snapshot().orElseThrow();
        assertThat(afterLaterUnion.zmin()).isEmpty();
        assertThat(afterLaterUnion.zmax()).isEmpty();
        assertThat(afterLaterUnion.mmin()).isEmpty();
        assertThat(afterLaterUnion.mmax()).isEmpty();
    }

    @Test
    void unionXyFoldsPoints() {
        BoundsAccumulator accumulator = new BoundsAccumulator();

        accumulator.unionXy(3, 4, 3, 4);
        accumulator.unionXy(-1, 8, -1, 8);
        accumulator.unionXy(7, 2, 7, 2);

        BoundingBox extent = accumulator.snapshot().orElseThrow();
        assertThat(extent.xmin()).isEqualTo(-1);
        assertThat(extent.ymin()).isEqualTo(2);
        assertThat(extent.xmax()).isEqualTo(7);
        assertThat(extent.ymax()).isEqualTo(8);
    }

    private static BoundingBox box(double xmin, double ymin, double xmax, double ymax) {
        return new BoundingBox(
                xmin,
                xmax,
                ymin,
                ymax,
                OptionalDouble.empty(),
                OptionalDouble.empty(),
                OptionalDouble.empty(),
                OptionalDouble.empty());
    }

    private static BoundingBox boxZ(double xmin, double ymin, double xmax, double ymax, double zmin, double zmax) {
        return new BoundingBox(
                xmin,
                xmax,
                ymin,
                ymax,
                OptionalDouble.of(zmin),
                OptionalDouble.of(zmax),
                OptionalDouble.empty(),
                OptionalDouble.empty());
    }

    private static BoundingBox boxZm(
            double xmin, double ymin, double xmax, double ymax, double zmin, double zmax, double mmin, double mmax) {
        return new BoundingBox(
                xmin,
                xmax,
                ymin,
                ymax,
                OptionalDouble.of(zmin),
                OptionalDouble.of(zmax),
                OptionalDouble.of(mmin),
                OptionalDouble.of(mmax));
    }
}
