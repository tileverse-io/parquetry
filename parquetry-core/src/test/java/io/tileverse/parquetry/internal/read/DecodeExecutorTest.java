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

import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;

class DecodeExecutorTest {

    @Test
    void runsSubmittedTaskAndReleasesItsSlotOnCompletion() throws Exception {
        DecodeExecutor executor = DecodeExecutor.ofParallelism(1);
        try {
            assertThat(executor.tryAcquire()).isTrue();
            Future<String> future = executor.submitAcquired(() -> "done");
            assertThat(future.get()).isEqualTo("done");
            assertThat(executor.availableSlots()).isEqualTo(1);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void boundsConcurrencyToParallelism() {
        DecodeExecutor executor = DecodeExecutor.ofParallelism(2);
        try {
            assertThat(executor.tryAcquire()).isTrue();
            assertThat(executor.tryAcquire()).isTrue();
            assertThat(executor.tryAcquire()).as("only 2 slots").isFalse();
            executor.release();
            assertThat(executor.tryAcquire()).isTrue();
            // release the two still-held slots so the pool is balanced before shutdown
            executor.release();
            executor.release();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void releasesSlotAndThrowsWhenSubmittingToAShutDownPool() {
        DecodeExecutor executor = DecodeExecutor.ofParallelism(1);
        executor.shutdownNow();
        assertThat(executor.tryAcquire()).isTrue();
        try {
            executor.submitAcquired(() -> "x");
        } catch (java.util.concurrent.RejectedExecutionException _) {
            // expected
        }
        assertThat(executor.availableSlots())
                .as("a rejected submit releases the slot it would have consumed")
                .isEqualTo(1);
    }

    @Test
    void releasesSlotWhenTaskThrows() throws Exception {
        DecodeExecutor executor = DecodeExecutor.ofParallelism(1);
        try {
            assertThat(executor.tryAcquire()).isTrue();
            Future<String> future = executor.submitAcquired(() -> {
                throw new RuntimeException("decode failure");
            });
            try {
                future.get();
            } catch (java.util.concurrent.ExecutionException _) {
                // the task's exception is wrapped in the Future; we only assert slot state here
            }
            assertThat(executor.availableSlots())
                    .as("slot must be released even when the task throws")
                    .isEqualTo(1);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void rejectsNonPositiveParallelism() {
        assertThatIllegalArgumentException().isThrownBy(() -> DecodeExecutor.ofParallelism(0));
    }

    @Test
    void sharedSingletonHasPositiveParallelism() {
        assertThat(DecodeExecutor.shared().parallelism()).isPositive();
    }
}
