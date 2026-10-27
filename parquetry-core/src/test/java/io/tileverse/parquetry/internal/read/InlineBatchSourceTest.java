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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;

class InlineBatchSourceTest {

    @Test
    void streamsBatchesInOrder() {
        ParquetRecordBatch b0 = TestBatches.emptyBatch();
        ParquetRecordBatch b1 = TestBatches.emptyBatch();
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
        AtomicBoolean emittedClosed = new AtomicBoolean();
        AtomicBoolean remainingClosed = new AtomicBoolean();
        ParquetRecordBatch emitted = TestBatches.emptyBatch(emittedClosed);
        ParquetRecordBatch remaining = TestBatches.emptyBatch(remainingClosed);
        InlineBatchSource source = new InlineBatchSource(new ListRowGroupDriver(List.of(emitted, remaining)));

        assertThat(source.next()).isSameAs(emitted);
        source.close();

        assertThat(remainingClosed.get())
                .as("the undrained batch is closed by close()")
                .isTrue();
        assertThat(emittedClosed.get())
                .as("the emitted batch is owned by the consumer")
                .isFalse();

        emitted.close();
    }
}
