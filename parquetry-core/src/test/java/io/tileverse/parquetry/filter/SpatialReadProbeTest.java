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
package io.tileverse.parquetry.filter;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.SpatialReadProbe.Decision;

class SpatialReadProbeTest {

    @Test
    void skipDescendKeepAreSharedSingletons() {
        assertThat(Decision.skip()).isSameAs(Decision.skip());
        assertThat(Decision.descend()).isSameAs(Decision.descend());
        assertThat(Decision.keep()).isSameAs(Decision.keep());
    }

    @Test
    void replaceHoldsTheGeometryItIsGiven() {
        Object geometry = new Object();
        Decision decision = Decision.replace(geometry);
        assertThat(decision).isInstanceOf(Decision.Replace.class);
        assertThat(((Decision.Replace) decision).geometry()).isSameAs(geometry);
    }

    @Test
    void aProbeIsConsultedWithRawDoubles() {
        SpatialReadProbe probe = (minX, minY, maxX, maxY) -> minX > 10 ? Decision.skip() : Decision.keep();
        assertThat(probe.probe(0, 0, 5, 5)).isSameAs(Decision.keep());
        assertThat(probe.probe(20, 0, 25, 5)).isSameAs(Decision.skip());
    }

    @Test
    void probeRegionDefaultsToDescend() {
        SpatialReadProbe leafOnly = (minX, minY, maxX, maxY) -> Decision.skip();
        assertThat(leafOnly.probeRegion(0, 0, 100, 100)).isSameAs(Decision.descend());
    }
}
