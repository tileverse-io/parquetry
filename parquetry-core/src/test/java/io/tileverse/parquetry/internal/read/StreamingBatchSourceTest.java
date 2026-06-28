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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.observe.SpillAccumulator;

class StreamingBatchSourceTest {

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void speculativeProducerReservesAndReleasesOnClose(@TempDir Path dir) throws InterruptedException {
        // Comfortably above each batch's heap bytes, which leaves headroom for all three to be held in heap.
        DecodeBudget budget = DecodeBudget.ofBytes(64L * 1024 * 1024);
        DiskBudget diskBudget = DiskBudget.ofBytes(1 << 20);
        BatchSpillStore store =
                new BatchSpillStore(dir, diskBudget, TestBatches.singleIntColumnSchema(), SpillAccumulator.NONE);
        List<ParquetRecordBatch> batches =
                List.of(TestBatches.intBatch(8), TestBatches.intBatch(8), TestBatches.intBatch(8));
        BatchHandoff handoff = new BatchHandoff(2);
        StreamingBatchSource source =
                new StreamingBatchSource(handoff, new ListRowGroupDriver(batches), budget, store, true);

        Thread producer = startProducer(source);

        List<ParquetRecordBatch> drained = drainAll(source);
        assertThat(drained)
                .as("all speculatively decoded batches are delivered")
                .hasSize(3);

        drained.forEach(ParquetRecordBatch::close);
        producer.join();
        store.close();

        assertThat(budget.available())
                .as("every batch reservation is released once the batch is closed")
                .isEqualTo(budget.capacity());
    }

    private static Thread startProducer(StreamingBatchSource source) {
        Thread producer = new Thread(source::runProducer, "streaming-batch-source-producer");
        producer.start();
        return producer;
    }

    private static List<ParquetRecordBatch> drainAll(StreamingBatchSource source) {
        List<ParquetRecordBatch> drained = new ArrayList<>();
        while (source.hasNext()) {
            drained.add(source.next());
        }
        return drained;
    }
}
