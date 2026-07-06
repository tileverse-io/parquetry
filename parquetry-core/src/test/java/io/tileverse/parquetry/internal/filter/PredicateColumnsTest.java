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
package io.tileverse.parquetry.internal.filter;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ColumnPath;

class PredicateColumnsTest {

    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");
    private static final ColumnPath GEOM = ColumnPath.of("geom");
    private static final ColumnPath N = ColumnPath.of("n");

    private static final Map<ColumnPath, ColumnPath> MAPPING = Map.of(GEOMETRY, GEOM, N, N);

    @Test
    void remapsRenamedLeafAndKeepsIdentityLeaf() {
        Predicate predicate = new Predicate.Eq(N, new Value.IntVal(7));
        Predicate remapped = PredicateColumns.remap(predicate, MAPPING);
        assertThat(remapped).isEqualTo(new Predicate.Eq(N, new Value.IntVal(7)));
    }

    @Test
    void remapsLeafColumnInEveryComparison() {
        Predicate predicate = new Predicate.Gt(GEOMETRY, new Value.IntVal(1));
        Predicate remapped = PredicateColumns.remap(predicate, MAPPING);
        assertThat(remapped).isEqualTo(new Predicate.Gt(GEOM, new Value.IntVal(1)));
    }

    @Test
    void leavesColumnAbsentFromMappingUnchanged() {
        ColumnPath other = ColumnPath.of("other");
        Predicate predicate = new Predicate.Eq(other, new Value.IntVal(3));
        Predicate remapped = PredicateColumns.remap(predicate, MAPPING);
        assertThat(remapped).isEqualTo(predicate);
    }

    @Test
    void recursesThroughAndOrNot() {
        Predicate predicate = new Predicate.And(List.of(
                new Predicate.Or(List.of(
                        new Predicate.Eq(GEOMETRY, new Value.IntVal(1)), new Predicate.Lt(N, new Value.IntVal(9)))),
                new Predicate.Not(new Predicate.IsNull(GEOMETRY))));

        Predicate remapped = PredicateColumns.remap(predicate, MAPPING);

        Predicate expected = new Predicate.And(List.of(
                new Predicate.Or(
                        List.of(new Predicate.Eq(GEOM, new Value.IntVal(1)), new Predicate.Lt(N, new Value.IntVal(9)))),
                new Predicate.Not(new Predicate.IsNull(GEOM))));
        assertThat(remapped).isEqualTo(expected);
    }

    @Test
    void remapsSpatialLeafColumn() {
        Bbox bbox = Bbox.of2d(0, 0, 10, 10);
        Predicate predicate = new Predicate.Spatial.BboxIntersects(GEOMETRY, bbox);
        Predicate remapped = PredicateColumns.remap(predicate, MAPPING);
        assertThat(remapped).isEqualTo(new Predicate.Spatial.BboxIntersects(GEOM, bbox));
    }

    @Test
    void leavesAlwaysUnchanged() {
        assertThat(PredicateColumns.remap(Predicate.ALWAYS_TRUE, MAPPING)).isEqualTo(Predicate.ALWAYS_TRUE);
        assertThat(PredicateColumns.remap(Predicate.ALWAYS_FALSE, MAPPING)).isEqualTo(Predicate.ALWAYS_FALSE);
    }
}
