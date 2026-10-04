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
package io.tileverse.parquetry.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.OptionalDouble;

import org.junit.jupiter.api.Test;

class BoundingBoxTest {

    @Test
    void aRegularOrWrappingBoxHasAnExtent() {
        BoundingBox regular =
                BoundingBox.builder().xmin(10).xmax(20).ymin(-5).ymax(5).build();
        BoundingBox wrapping =
                BoundingBox.builder().xmin(170).xmax(-170).ymin(-5).ymax(5).build();

        assertThat(regular.hasExtent()).isTrue();
        assertThat(wrapping.hasExtent()).isTrue();
    }

    @Test
    void anEmptyGeometryEnvelopeHasNoExtent() {
        BoundingBox empty = BoundingBox.builder()
                .xmin(Double.POSITIVE_INFINITY)
                .xmax(Double.NEGATIVE_INFINITY)
                .ymin(Double.POSITIVE_INFINITY)
                .ymax(Double.NEGATIVE_INFINITY)
                .build();
        BoundingBox invertedLatitudes =
                BoundingBox.builder().xmin(0).xmax(10).ymin(5).ymax(-5).build();
        BoundingBox nan =
                BoundingBox.builder().xmin(Double.NaN).xmax(10).ymin(0).ymax(5).build();

        assertThat(empty.hasExtent()).isFalse();
        assertThat(invertedLatitudes.hasExtent()).isFalse();
        assertThat(nan.hasExtent()).isFalse();
    }

    /** The inverted infinite box of empty geometries has its minimum east of its maximum, yet bounds no longitudes. */
    @Test
    void aBoxWithoutAnExtentNeverWraps() {
        BoundingBox empty = BoundingBox.builder()
                .xmin(Double.POSITIVE_INFINITY)
                .xmax(Double.NEGATIVE_INFINITY)
                .ymin(Double.POSITIVE_INFINITY)
                .ymax(Double.NEGATIVE_INFINITY)
                .build();

        assertThat(empty.wrapsAntimeridian()).isFalse();
        assertThat(empty.planarEnclosure()).isSameAs(empty);
    }

    @Test
    void aBoxWithinTheLongitudeRangeIsItsOwnPlanarEnclosure() {
        BoundingBox regular =
                BoundingBox.builder().xmin(10).xmax(20).ymin(-5).ymax(5).build();

        assertThat(regular.wrapsAntimeridian()).isFalse();
        assertThat(regular.planarEnclosure()).isEqualTo(regular);
    }

    @Test
    void aWrappingBoxEnclosesTheFullLongitudeRangeWithItsOwnOtherExtents() {
        BoundingBox wrapping = new BoundingBox(
                170,
                -170,
                -5,
                5,
                OptionalDouble.of(1),
                OptionalDouble.of(2),
                OptionalDouble.of(3),
                OptionalDouble.of(4));

        BoundingBox enclosure = wrapping.planarEnclosure();

        assertThat(wrapping.wrapsAntimeridian()).isTrue();
        assertThat(enclosure)
                .isEqualTo(new BoundingBox(
                        -180,
                        180,
                        -5,
                        5,
                        OptionalDouble.of(1),
                        OptionalDouble.of(2),
                        OptionalDouble.of(3),
                        OptionalDouble.of(4)));
        assertThat(enclosure.wrapsAntimeridian()).isFalse();
    }
}
