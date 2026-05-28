/*
 * Copyright (c) 2026 Tileverse.io
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

import io.tileverse.parquetry.schema.ColumnPath;

class SpatialPredicateTest {
    private static final ColumnPath GEOM = ColumnPath.of("geometry");
    private static final Bbox Q = Bbox.of2d(0, 0, 10, 10);

    @Test
    void buildersProduceTheFourRelations() {
        assertThat(Pred.col(GEOM).bboxIntersects(Q)).isInstanceOf(Predicate.Spatial.BboxIntersects.class);
        assertThat(Pred.col(GEOM).bboxContains(Q)).isInstanceOf(Predicate.Spatial.BboxContains.class);
        assertThat(Pred.col(GEOM).bboxCoveredBy(Q)).isInstanceOf(Predicate.Spatial.BboxCoveredBy.class);
        assertThat(Pred.col(GEOM).bboxEquals(Q)).isInstanceOf(Predicate.Spatial.BboxEquals.class);
    }

    @Test
    void spatialExposesColumnAndQueryBbox() {
        Predicate.Spatial s = (Predicate.Spatial) Pred.col(GEOM).bboxContains(Q);
        assertThat(s.col()).isEqualTo(GEOM);
        assertThat(s.bbox()).isEqualTo(Q);
    }

    @Test
    void intersectsBuilderMatchesLegacyAlias() {
        assertThat(Pred.col(GEOM).intersects(Q)).isInstanceOf(Predicate.Spatial.BboxIntersects.class);
    }
}
