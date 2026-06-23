/*
 * Copyright (c) 2026 Multivers.io
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

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.schema.ColumnPath;

class QueryTest {

    @Test
    void ofDefaultsToTheIdentityOutputShape() {
        Query query = Query.of(Predicate.ALWAYS_TRUE, Projection.ALL);
        assertThat(query.predicate()).isEqualTo(Predicate.ALWAYS_TRUE);
        assertThat(query.projection()).isEqualTo(Projection.ALL);
        assertThat(query.output()).isEmpty();
    }

    @Test
    void keepsOutputColumnsInOrder() {
        OutputColumn a = new OutputColumn.Constant(ColumnPath.of("year"), new Value.IntVal(2024));
        OutputColumn b = new OutputColumn.Constant(ColumnPath.of("month"), new Value.IntVal(1));
        Query query = new Query(Predicate.ALWAYS_TRUE, Projection.ALL, List.of(a, b));
        assertThat(query.output()).containsExactly(a, b);
    }

    @Test
    void outputColumnsAreDefensivelyCopied() {
        List<OutputColumn> mutable = new ArrayList<>();
        mutable.add(new OutputColumn.Constant(ColumnPath.of("year"), new Value.IntVal(2024)));
        Query query = new Query(Predicate.ALWAYS_TRUE, Projection.ALL, mutable);
        mutable.clear();
        assertThat(query.output()).hasSize(1);
    }
}
