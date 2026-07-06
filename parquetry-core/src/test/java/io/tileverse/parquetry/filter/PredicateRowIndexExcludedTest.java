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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.schema.ColumnPath;

class PredicateRowIndexExcludedTest {

    // The caller names the synthesized row-position column; the engine mandates no fixed name.
    private static final ColumnPath POS = ColumnPath.of("_pos");

    @Test
    void columnsReferencesTheRowPositionColumn() {
        RowPositionSet deleted = SortedLongPositionSet.of(new long[] {1, 2});
        Predicate predicate = new Predicate.RowIndexExcluded(POS, deleted);

        assertThat(Predicate.columns(predicate)).isEqualTo(Set.of(POS));
    }

    @Test
    void rejectsNullArguments() {
        RowPositionSet deleted = SortedLongPositionSet.of(new long[] {0});

        assertThatThrownBy(() -> new Predicate.RowIndexExcluded(null, deleted))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new Predicate.RowIndexExcluded(POS, null)).isInstanceOf(NullPointerException.class);
    }
}
