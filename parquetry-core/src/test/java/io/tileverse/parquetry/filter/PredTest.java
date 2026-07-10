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
package io.tileverse.parquetry.filter;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.schema.ColumnPath;

class PredTest {

    private static final ColumnPath ID = ColumnPath.of("id");

    @Test
    void eqUuidBuildsUuidVal() {
        UUID uuid = UUID.fromString("0123456f-89ab-cdef-fedc-ba9876543210");
        assertThat(Pred.col(ID).eq(uuid)).isEqualTo(new Predicate.Eq(ID, new Value.UuidVal(uuid)));
    }

    @Test
    void inUuidsBuildsInOfUuidVals() {
        UUID a = new UUID(1L, 1L);
        UUID b = new UUID(2L, 2L);
        assertThat(Pred.col(ID).inUuids(a, b))
                .isEqualTo(new Predicate.In(ID, List.of(new Value.UuidVal(a), new Value.UuidVal(b))));
    }

    @Test
    void orderedUuidFactories() {
        UUID uuid = new UUID(7L, 7L);
        assertThat(Pred.col(ID).lt(uuid)).isEqualTo(new Predicate.Lt(ID, new Value.UuidVal(uuid)));
        assertThat(Pred.col(ID).gtEq(uuid)).isEqualTo(new Predicate.GtEq(ID, new Value.UuidVal(uuid)));
    }
}
