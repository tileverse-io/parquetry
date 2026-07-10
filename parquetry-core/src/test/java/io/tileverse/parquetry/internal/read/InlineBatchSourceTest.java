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
package io.tileverse.parquetry.internal.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.Arena;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;

class InlineBatchSourceTest {

    @Test
    void streamsBatchesInOrder() {
        ParquetRecordBatch b0 = TestBatches.emptyBatch(Arena.ofConfined());
        ParquetRecordBatch b1 = TestBatches.emptyBatch(Arena.ofConfined());
        InlineBatchSource source = new InlineBatchSource(new ListRowGroupDriver(List.of(b0, b1)));

        assertThat(source.hasNext()).isTrue();
        assertThat(source.next()).isSameAs(b0);
        assertThat(source.next()).isSameAs(b1);
        assertThat(source.hasNext()).isFalse();

        source.close();
        b0.close();
        b1.close();
    }

    @Test
    void closeAfterPartialDrainClosesRemainingBatch() {
        Arena emittedArena = Arena.ofConfined();
        Arena remainingArena = Arena.ofConfined();
        ParquetRecordBatch emitted = TestBatches.emptyBatch(emittedArena);
        ParquetRecordBatch remaining = TestBatches.emptyBatch(remainingArena);
        InlineBatchSource source = new InlineBatchSource(new ListRowGroupDriver(List.of(emitted, remaining)));

        assertThat(source.next()).isSameAs(emitted);
        source.close();

        assertThatThrownBy(() -> remainingArena.allocate(8))
                .as("the undrained batch is closed by close()")
                .isInstanceOf(IllegalStateException.class);
        assertThat(emittedArena.allocate(8))
                .as("the emitted batch is owned by the consumer")
                .isNotNull();

        emitted.close();
    }
}
