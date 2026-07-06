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

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.schema.ColumnPath;

class PredBuilderTest {

    @Test
    void builderEmitsSamePredicateAsManualConstruction() {
        Predicate fluent = col("year").gtEq(2020).and(col("country").eq("AR"));

        Predicate manual = new Predicate.And(List.of(
                new Predicate.GtEq(ColumnPath.of("year"), new Value.IntVal(2020)),
                new Predicate.Eq(ColumnPath.of("country"), new Value.StringVal("AR"))));

        assertThat(fluent).isEqualTo(manual);
    }

    @Test
    void chainedAndOrNot() {
        Predicate p = col("year")
                .gtEq(2020)
                .and(col("country").eq("AR"))
                .or(col("year").lt(2010).negate());
        assertThat(p).isInstanceOf(Predicate.Or.class);
    }

    @Test
    void inIntsBuildsListPredicate() {
        Predicate p = col("year").inInts(2020, 2021, 2022);
        assertThat(p).isInstanceOf(Predicate.In.class);
        Predicate.In in = (Predicate.In) p;
        assertThat(in.values()).hasSize(3);
    }

    @Test
    void bboxIntersects() {
        Predicate p = col("geom").intersects(Bbox.of2d(0, 0, 10, 10));
        assertThat(p).isInstanceOf(Predicate.Spatial.BboxIntersects.class);
    }

    @Test
    void nestedColumnPath() {
        Predicate p = Pred.col("address", "city").eq("Buenos Aires");
        Predicate.Eq eq = (Predicate.Eq) p;
        assertThat(eq.col().dot()).isEqualTo("address.city");
    }
}
