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
package io.tileverse.parquetry.internal.filter;

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.schema.ColumnPath;

class PagePrunableTest {

    private static final Predicate IN_THE_BOX =
            new Predicate.Spatial.BboxIntersects(ColumnPath.of("geometry"), Bbox.of2d(-1, -1, 1, 1));

    @Test
    void columnsNameEachComparisonLeaf() {
        Predicate predicate =
                new Predicate.And(List.of(col("year").eq(2020), col("count").gt(3)));

        assertThat(prunableColumns(predicate)).containsExactly(ColumnPath.of("year"), ColumnPath.of("count"));
    }

    // A spatial leaf has no page statistics: naming its column would read sections for nothing.
    @Test
    void conjunctOutOfReachIsDropped() {
        Predicate xmin = col("bbox", "xmin").lt(1.0);
        Predicate predicate = new Predicate.And(List.of(IN_THE_BOX, xmin));

        assertThat(PagePrunable.part(predicate)).contains(new Predicate.And(List.of(xmin)));
        assertThat(prunableColumns(predicate)).containsExactly(ColumnPath.of("bbox", "xmin"));
    }

    // A disjunction with a branch out of reach matches any row: reading the sections of its other branches is waste.
    @Test
    void orOutOfReachHasNoPart() {
        Predicate predicate = new Predicate.Or(List.of(IN_THE_BOX, col("year").eq(2020)));

        assertThat(PagePrunable.part(predicate)).isEmpty();
        assertThat(prunableColumns(predicate)).isEmpty();
    }

    @Test
    void orWithinReachIsKept() {
        Predicate predicate =
                new Predicate.Or(List.of(col("year").eq(2020), col("count").gt(3)));

        assertThat(PagePrunable.part(predicate)).contains(predicate);
    }

    @Test
    void negationHasNoPart() {
        Predicate negation = PredicateNormalizer.normalize(IN_THE_BOX.negate());

        assertThat(PagePrunable.part(negation)).isEmpty();
    }

    private static List<ColumnPath> prunableColumns(Predicate predicate) {
        return PagePrunable.part(predicate).map(PagePrunable::columns).orElse(List.of());
    }
}
