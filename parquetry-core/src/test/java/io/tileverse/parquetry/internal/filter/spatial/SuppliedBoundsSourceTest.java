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
package io.tileverse.parquetry.internal.filter.spatial;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.OptionalDouble;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.schema.ColumnPath;

class SuppliedBoundsSourceTest {

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

    @Test
    void servesFileBoundsForKnownColumnAndEmptyForOthers() {
        ColumnPath geom = ColumnPath.of("geom");
        BoundingBox bounds = box(-10, -5, 10, 5);
        SuppliedBoundsSource source = new SuppliedBoundsSource(Map.of(geom, bounds));

        assertThat(source.fileBounds(geom)).contains(bounds);
        assertThat(source.rowGroupBounds(geom, 0)).contains(bounds);
        assertThat(source.rowGroupBounds(geom, 7)).contains(bounds);
        assertThat(source.fileBounds(ColumnPath.of("other"))).isEmpty();
    }
}
