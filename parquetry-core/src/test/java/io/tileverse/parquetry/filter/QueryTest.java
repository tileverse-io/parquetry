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
    void ofDefaultsToTheIdentityOutputSelection() {
        Query query = Query.of(Predicate.ALWAYS_TRUE, Projection.ALL);
        assertThat(query.predicate()).isEqualTo(Predicate.ALWAYS_TRUE);
        assertThat(query.projection()).isEqualTo(Projection.ALL);
        assertThat(query.outputColumns()).isEmpty();
    }

    @Test
    void keepsOutputColumnsInOrder() {
        ColumnPath year = ColumnPath.of("year");
        ColumnPath month = ColumnPath.of("month");
        Query query = Query.builder(Predicate.ALWAYS_TRUE, Projection.ALL)
                .outputColumns(List.of(year, month))
                .build();
        assertThat(query.outputColumns()).containsExactly(year, month);
    }

    @Test
    void outputColumnsAreDefensivelyCopied() {
        List<ColumnPath> mutable = new ArrayList<>();
        mutable.add(ColumnPath.of("year"));
        Query query = Query.builder(Predicate.ALWAYS_TRUE, Projection.ALL)
                .outputColumns(mutable)
                .build();
        mutable.clear();
        assertThat(query.outputColumns()).hasSize(1);
    }
}
