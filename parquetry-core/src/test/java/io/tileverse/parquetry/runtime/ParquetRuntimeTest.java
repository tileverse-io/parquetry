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
package io.tileverse.parquetry.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.io.limits.IoLimits;
import io.tileverse.parquetry.io.limits.ResourceLimits;

class ParquetRuntimeTest {

    private static final long MIB = 1L << 20;

    @Test
    void defaultRuntimeIsElasticAndPositive() {
        ParquetRuntime rt = ParquetRuntime.defaultRuntime();
        assertThat(rt.decodeBudget().capacity()).isPositive();
        assertThat(rt.diskBudget().capacity()).isPositive();
        assertThat(rt.maxDecodeAhead()).isGreaterThanOrEqualTo(2);
        assertThat(rt.spillEnabled()).isTrue();
        assertThat(rt.spillDir()).isEqualTo(Path.of(System.getProperty("java.io.tmpdir")));
        assertThat(rt.prefetchDepth()).isEqualTo(2);
        assertThat(rt.maxConcurrentFetchesPerRead()).isEqualTo(4);
    }

    @Test
    void defaultsProvideConservativeFetchTunables() {
        ParquetRuntime rt = ParquetRuntime.defaultRuntime();
        assertThat(rt.maxCoalesceGap()).isEqualTo(1 << 20);
        assertThat(rt.maxCoalescedSpan()).isEqualTo(8 << 20);
        assertThat(rt.fetchBudget().capacity())
                .isEqualTo(IoLimits.from(ResourceLimits.getDefault()).maxOffHeapBytes());
        assertThat(rt.computeExecutor()).isSameAs(ComputeExecutor.shared());
        assertThat(rt.decodeBudget().capacity())
                .isEqualTo(DecodeBudget.defaultBudget().capacity());
    }

    @Test
    void rejectsNonPositiveScalars() {
        ParquetRuntime.Builder span = ParquetRuntime.builder().maxCoalescedSpan(0);
        assertThatIllegalArgumentException().isThrownBy(span::build);

        ParquetRuntime.Builder fetches = ParquetRuntime.builder().maxConcurrentFetchesPerRead(0);
        assertThatIllegalArgumentException().isThrownBy(fetches::build);

        ParquetRuntime.Builder gap = ParquetRuntime.builder().maxCoalesceGap(-1);
        assertThatIllegalArgumentException().isThrownBy(gap::build);

        ParquetRuntime.Builder builder = ParquetRuntime.builder();
        assertThatIllegalArgumentException().isThrownBy(() -> builder.maxDecodeAhead(-1));
    }

    @Test
    void budgetAndSpillOverridesAreHeld() {
        DecodeBudget decode = DecodeBudget.ofBytes(123_456);
        DiskBudget disk = DiskBudget.ofBytes(1234);
        Path spillDir = Path.of(System.getProperty("java.io.tmpdir"), "parquetry-spill-test");
        ParquetRuntime rt = ParquetRuntime.builder()
                .decodeBudget(decode)
                .diskBudget(disk)
                .spillDir(spillDir)
                .spillEnabled(false)
                .build();
        assertThat(rt.decodeBudget()).isSameAs(decode);
        assertThat(rt.diskBudget()).isSameAs(disk);
        assertThat(rt.spillDir()).isEqualTo(spillDir);
        assertThat(rt.spillEnabled()).isFalse();
    }

    @Test
    void maxDecodeAheadScalesWithTheConfiguredDecodeBudget() {
        ParquetRuntime tiny =
                ParquetRuntime.builder().decodeBudget(DecodeBudget.ofBytes(1)).build();
        assertThat(tiny.maxDecodeAhead()).isEqualTo(2);
    }

    @Test
    void roomyDecodeBudgetExceedsTheFloorOnAMultiCoreBox() {
        DecodeBudget roomy = DecodeBudget.ofBytes(2048L << 20); // heapTerm 64
        ParquetRuntime rt = ParquetRuntime.builder().decodeBudget(roomy).build();
        int cores = Runtime.getRuntime().availableProcessors();
        if (cores > 2) {
            assertThat(rt.maxDecodeAhead()).isGreaterThan(2);
        }
    }

    static Stream<Arguments> decodeAheadCases() {
        return Stream.of(
                // cores, decodeBudgetCapacity, expected, why
                Arguments.of(1, 256 * MIB, 2, "low cores held to the floor of 2"),
                Arguments.of(2, 2048 * MIB, 2, "cores at the floor"),
                Arguments.of(4, 256 * MIB, 3, "4-core / 1g pod: capped at the decode-ahead ceiling of 3"),
                Arguments.of(16, 256 * MIB, 3, "16-core / 1g pod: capped at the decode-ahead ceiling of 3"),
                Arguments.of(8, 256 * MIB, 3, "8-core / 1g pod: capped at the decode-ahead ceiling of 3"),
                Arguments.of(16, 2048 * MIB, 3, "16-core / 8g pod: capped at the decode-ahead ceiling of 3"),
                Arguments.of(16, 1L, 2, "tiny budget: heapTerm held to the floor of 2"));
    }

    @ParameterizedTest(name = "[{index}] cores={0} budget={1}B -> {2} ({3})")
    @MethodSource("decodeAheadCases")
    @DisplayName("decode-ahead default clamps processors into [2, min(decodeBudget/32MiB, 3)]")
    void decodeAheadDefaultClampsCoresAgainstHeapTerm(int cores, long decodeBudgetCapacity, int expected, String why) {
        assertThat(ParquetRuntime.decodeAheadDefault(cores, decodeBudgetCapacity))
                .as(why)
                .isEqualTo(expected);
    }

    @Test
    void withPrefetchDepthSharesResourcesAndSwapsOnlyTheScalar() {
        ParquetRuntime base = ParquetRuntime.defaultRuntime();
        ParquetRuntime derived = base.withPrefetchDepth(1);
        assertThat(derived.prefetchDepth()).isEqualTo(1);
        assertThat(derived.segmentPool()).isSameAs(base.segmentPool());
        assertThat(derived.computeExecutor()).isSameAs(base.computeExecutor());
        assertThat(derived.decodeBudget()).isSameAs(base.decodeBudget());
        assertThat(derived.diskBudget()).isSameAs(base.diskBudget());
    }

    @Test
    void builderRejectsInvalidScalars() {
        ParquetRuntime.Builder builder = ParquetRuntime.builder().prefetchDepth(-1);
        assertThatThrownBy(builder::build).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void resourceLimitsSizeTheBudgets() {
        ResourceLimits fake =
                ResourceLimits.fixed(1L << 30, 8L << 30, 4, Path.of(System.getProperty("java.io.tmpdir")));
        ParquetRuntime runtime = ParquetRuntime.builder().resourceLimits(fake).build();
        assertThat(runtime.fetchBudget().capacity()).as("0.1 of 1 GiB").isEqualTo((long) (0.1 * (1L << 30)));
        assertThat(runtime.diskBudget().capacity()).as("0.5 of 8 GiB").isEqualTo((long) (0.5 * (8L << 30)));
    }

    @Test
    void explicitBudgetOverridesResourceLimits() {
        ResourceLimits fake =
                ResourceLimits.fixed(1L << 30, 8L << 30, 4, Path.of(System.getProperty("java.io.tmpdir")));
        FetchBudget explicit = FetchBudget.ofBytes(123_456);
        ParquetRuntime runtime = ParquetRuntime.builder()
                .resourceLimits(fake)
                .fetchBudget(explicit)
                .build();
        assertThat(runtime.fetchBudget()).isSameAs(explicit);
    }
}
