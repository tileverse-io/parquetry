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
package io.tileverse.parquetry.internal.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.PruningDecision;
import io.tileverse.parquetry.filter.Tier;

class ReadOptionsTest {

    @Test
    void defaultsTurnAllFiltersOn() {
        ReadOptions opts = ReadOptions.DEFAULTS;
        assertThat(opts.useStatsFilter()).isTrue();
        assertThat(opts.useDictionaryFilter()).isTrue();
        assertThat(opts.useColumnIndexFilter()).isTrue();
        assertThat(opts.useBloomFilter()).isTrue();
        assertThat(opts.useRecordLevelFilter()).isTrue();
    }

    @Test
    void builderOverridesIndividualFlags() {
        ReadOptions opts = ReadOptions.builder().useStatsFilter(false).build();
        assertThat(opts.useStatsFilter()).isFalse();
        assertThat(opts.useDictionaryFilter()).isTrue(); // unchanged
    }

    @Test
    void listenerReceivesEachDecision() {
        List<PruningDecision> seen = new ArrayList<>();
        ReadOptions opts =
                ReadOptions.builder().pruningDecisionListener(seen::add).build();
        PruningDecision d = new PruningDecision.NotApplied(Tier.STATS, "test");
        opts.pruningDecisionListener().accept(d);
        assertThat(seen).containsExactly(d);
    }

    @Test
    void zeroBatchSizeRejected() {
        ReadOptions.Builder builder = ReadOptions.builder();
        assertThatThrownBy(() -> builder.batchSize(0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void decryptionKeyRetrieverDefaultsToEmpty() {
        assertThat(ReadOptions.DEFAULTS.decryptionKeyRetriever()).isEmpty();
    }

    @Test
    void decryptionKeyRetrieverCanBeSet() {
        DecryptionKeyRetriever stub = meta -> new byte[16];
        ReadOptions opts = ReadOptions.builder().decryptionKeyRetriever(stub).build();
        assertThat(opts.decryptionKeyRetriever()).contains(stub);
    }

    @Test
    void defaultsProvideConservativeFetchTunables() {
        ReadOptions options = ReadOptions.DEFAULTS;
        assertThat(options.maxCoalesceGap()).isEqualTo(1 << 20);
        assertThat(options.maxCoalescedSpan()).isEqualTo(8 << 20);
        assertThat(options.prefetchDepth()).isEqualTo(2);
        assertThat(options.maxConcurrentFetchesPerRead()).isEqualTo(4);
        assertThat(options.fetchBudget()).isSameAs(FetchBudget.defaultBudget());
    }

    @Test
    void rejectsNonPositiveFetchTunables() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ReadOptions.builder().maxCoalescedSpan(0));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ReadOptions.builder().prefetchDepth(-1));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ReadOptions.builder().maxConcurrentFetchesPerRead(0));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ReadOptions.builder().maxCoalesceGap(-1));
    }

    @Test
    void defaultsProvideParallelDecodeTunables() {
        ReadOptions options = ReadOptions.DEFAULTS;
        assertThat(options.decodeExecutor()).isSameAs(DecodeExecutor.shared());
        assertThat(options.maxDecodeAheadPerRead()).isEqualTo(2);
    }

    @Test
    void builderOverridesParallelDecodeTunables() {
        DecodeExecutor executor = DecodeExecutor.ofParallelism(3);
        try {
            ReadOptions options = ReadOptions.builder()
                    .decodeExecutor(executor)
                    .maxDecodeAheadPerRead(0)
                    .build();
            assertThat(options.decodeExecutor()).isSameAs(executor);
            assertThat(options.maxDecodeAheadPerRead()).isZero();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void rejectsNegativeDecodeAhead() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> ReadOptions.builder().maxDecodeAheadPerRead(-1));
    }

    @Test
    void defaultDecodeBudgetIsTheSharedDefault() {
        assertThat(ReadOptions.DEFAULTS.decodeBudget().capacity())
                .isEqualTo(DecodeBudget.defaultBudget().capacity());
    }

    @Test
    void decodeBudgetOverrideIsHonored() {
        DecodeBudget custom = DecodeBudget.ofBytes(123_456);
        ReadOptions options = ReadOptions.builder().decodeBudget(custom).build();
        assertThat(options.decodeBudget()).isSameAs(custom);
    }
}
