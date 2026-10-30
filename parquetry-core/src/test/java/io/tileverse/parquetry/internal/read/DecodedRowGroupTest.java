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

class DecodedRowGroupTest {

    @Test
    void emitsBatchesInOrder() {
        ParquetRecordBatch b0 = TestBatches.emptyBatch();
        ParquetRecordBatch b1 = TestBatches.emptyBatch();
        DecodedRowGroup rowGroup = new DecodedRowGroup(new ListBatchSource(List.of(b0, b1)), true);

        assertThat(rowGroup.hasNext()).isTrue();
        assertThat(rowGroup.next()).isSameAs(b0);
        assertThat(rowGroup.next()).isSameAs(b1);
        assertThat(rowGroup.hasNext()).isFalse();

        b0.close();
        b1.close();
    }

    @Test
    void closeClosesOnlyUnemittedBatches() {
        AtomicBoolean emittedClosed = new AtomicBoolean();
        AtomicBoolean unemittedClosed = new AtomicBoolean();
        ParquetRecordBatch emitted = TestBatches.emptyBatch(emittedClosed);
        ParquetRecordBatch unemitted = TestBatches.emptyBatch(unemittedClosed);
        DecodedRowGroup rowGroup = new DecodedRowGroup(new ListBatchSource(List.of(emitted, unemitted)), true);

        rowGroup.next(); // emit the first; the consumer now owns it
        rowGroup.close();

        assertThat(unemittedClosed.get())
                .as("the unemitted batch is closed by close()")
                .isTrue();
        assertThat(emittedClosed.get())
                .as("the emitted batch is owned by its consumer")
                .isFalse();

        emitted.close();
    }

    @Test
    void closeIsIdempotent() {
        AtomicBoolean closed = new AtomicBoolean();
        ParquetRecordBatch only = TestBatches.emptyBatch(closed);
        DecodedRowGroup rowGroup = new DecodedRowGroup(new ListBatchSource(List.of(only)), true);
        rowGroup.close();
        rowGroup.close();
        assertThat(closed.get()).isTrue();
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

}
