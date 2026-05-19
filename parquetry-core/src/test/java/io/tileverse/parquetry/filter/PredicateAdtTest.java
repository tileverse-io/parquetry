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

import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.schema.ColumnPath;

class PredicateAdtTest {

    @Test
    void allVariantsConstruct() {
        ColumnPath year = ColumnPath.of("year");
        List<Predicate> all = List.of(
                Predicate.ALWAYS_TRUE,
                Predicate.ALWAYS_FALSE,
                new Predicate.Eq(year, new Value.IntVal(2020)),
                new Predicate.NotEq(year, new Value.IntVal(2020)),
                new Predicate.Lt(year, new Value.IntVal(2020)),
                new Predicate.LtEq(year, new Value.IntVal(2020)),
                new Predicate.Gt(year, new Value.IntVal(2020)),
                new Predicate.GtEq(year, new Value.IntVal(2020)),
                new Predicate.In(year, List.of(new Value.IntVal(2020), new Value.IntVal(2021))),
                new Predicate.IsNull(year),
                new Predicate.IsNotNull(year),
                new Predicate.And(List.of(Predicate.ALWAYS_TRUE, Predicate.ALWAYS_TRUE)),
                new Predicate.Or(List.of(Predicate.ALWAYS_TRUE, Predicate.ALWAYS_FALSE)),
                new Predicate.Not(Predicate.ALWAYS_FALSE),
                new Predicate.BboxIntersects(ColumnPath.of("geom"), Bbox.of2d(0, 0, 1, 1)));
        assertThat(all).hasSize(15);
    }

    @Test
    @SuppressWarnings("java:S7475") // palantirJavaFormat 2.90 cannot parse bare _ in nested record patterns
    void patternMatchingExhaustive() {
        Predicate p = new Predicate.Eq(ColumnPath.of("year"), new Value.IntVal(2020));
        String description =
                switch (p) {
                    case Predicate.Always(boolean value) -> "always(" + value + ")";
                    case Predicate.Eq(ColumnPath col, Value _) -> "eq(" + col.dot() + ")";
                    case Predicate.NotEq _ -> "neq";
                    case Predicate.Lt _ -> "lt";
                    case Predicate.LtEq _ -> "le";
                    case Predicate.Gt _ -> "gt";
                    case Predicate.GtEq _ -> "ge";
                    case Predicate.In _ -> "in";
                    case Predicate.IsNull _ -> "null";
                    case Predicate.IsNotNull _ -> "notnull";
                    case Predicate.And _ -> "and";
                    case Predicate.Or _ -> "or";
                    case Predicate.Not _ -> "not";
                    case Predicate.BboxIntersects _ -> "bbox";
                };
        assertThat(description).isEqualTo("eq(year)");
    }

    @Test
    void binaryValueIsReadOnly() {
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.wrap(new byte[] {1, 2, 3});
        Value.BinaryVal v = new Value.BinaryVal(buf);
        assertThat(v.value().isReadOnly()).isTrue();
    }

    @Test
    void bbox2dIntersectsWorks() {
        Bbox a = Bbox.of2d(0, 0, 10, 10);
        Bbox b = Bbox.of2d(5, 5, 15, 15);
        Bbox c = Bbox.of2d(20, 20, 30, 30);
        assertThat(a.intersects(b)).isTrue();
        assertThat(a.intersects(c)).isFalse();
    }

    @Test
    void bbox3dIntersectsWorks() {
        Bbox a = Bbox.of3d(0, 0, 0, 10, 10, 10);
        Bbox b = Bbox.of3d(5, 5, 5, 15, 15, 15);
        Bbox cZmiss = Bbox.of3d(5, 5, 20, 15, 15, 30);
        assertThat(a.intersects(b)).isTrue();
        assertThat(a.intersects(cZmiss)).isFalse();
    }

    @Test
    void projectionOf() {
        Projection p = Projection.of(java.util.Set.of(ColumnPath.of("a"), ColumnPath.of("b")));
        assertThat(p).isInstanceOf(Projection.Columns.class);
        assertThat(((Projection.Columns) p).kept()).hasSize(2);

        Projection all = Projection.ALL;
        assertThat(all).isInstanceOf(Projection.All.class);
    }
}
