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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.MatchAction;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ColumnPath;

class QuantifiedNormalizeTest {

    private static final ColumnPath LEAF = ColumnPath.of("addresses", "list", "element", "locality");

    private static Value x() {
        return new Value.StringVal("X");
    }

    @Test
    void notOverAnyBecomesAllWithFlippedOperator() {
        Predicate any = new Predicate.Quantified(MatchAction.ANY, new Predicate.Eq(LEAF, x()));
        Predicate normalized = PredicateNormalizer.normalize(new Predicate.Not(any));
        assertThat(normalized).isEqualTo(new Predicate.Quantified(MatchAction.ALL, new Predicate.NotEq(LEAF, x())));
    }

    @Test
    void notOverAllBecomesAnyWithFlippedOperator() {
        Predicate all = new Predicate.Quantified(MatchAction.ALL, new Predicate.Eq(LEAF, x()));
        Predicate normalized = PredicateNormalizer.normalize(new Predicate.Not(all));
        assertThat(normalized).isEqualTo(new Predicate.Quantified(MatchAction.ANY, new Predicate.NotEq(LEAF, x())));
    }

    @Test
    void notOverOneStaysWrapped() {
        Predicate one = new Predicate.Quantified(MatchAction.ONE, new Predicate.Eq(LEAF, x()));
        Predicate normalized = PredicateNormalizer.normalize(new Predicate.Not(one));
        assertThat(normalized).isEqualTo(new Predicate.Not(one));
    }

    @Test
    void plainQuantifiedNormalizesUnchanged() {
        Predicate any = new Predicate.Quantified(MatchAction.ANY, new Predicate.Eq(LEAF, x()));
        assertThat(PredicateNormalizer.normalize(any)).isEqualTo(any);
    }
}
