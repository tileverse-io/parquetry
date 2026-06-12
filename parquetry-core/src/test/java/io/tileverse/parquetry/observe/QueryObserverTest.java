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
package io.tileverse.parquetry.observe;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.explain.Tier;

class QueryObserverTest {

    @Test
    void noneIsANoOpAndDoesNotWantTimings() {
        QueryObserver none = QueryObserver.NONE;

        assertThat(none.wantsTimings()).isFalse();
        none.onRowGroupPlanned(0, new PruningDecision.PassedAll(Tier.STATS, "ok"));
    }

    @Test
    void compositeFansOutEveryCallbackToEachObserver() {
        List<String> calls = new ArrayList<>();
        QueryObserver one = new QueryObserver() {
            @Override
            public void onRowGroupPlanned(int rowGroup, PruningDecision decision) {
                calls.add("one:" + rowGroup);
            }
        };
        QueryObserver two = new QueryObserver() {
            @Override
            public void onRowGroupPlanned(int rowGroup, PruningDecision decision) {
                calls.add("two:" + rowGroup);
            }
        };

        QueryObserver composite = QueryObserver.composite(one, two);
        composite.onRowGroupPlanned(7, new PruningDecision.PassedAll(Tier.STATS, "ok"));

        assertThat(calls).containsExactly("one:7", "two:7");
    }

    @Test
    void compositeWantsTimingsWhenAnyMemberDoes() {
        QueryObserver wants = new QueryObserver() {
            @Override
            public boolean wantsTimings() {
                return true;
            }
        };

        assertThat(QueryObserver.composite(QueryObserver.NONE, wants).wantsTimings())
                .isTrue();
        assertThat(QueryObserver.composite(QueryObserver.NONE, QueryObserver.NONE)
                        .wantsTimings())
                .isFalse();
    }
}
