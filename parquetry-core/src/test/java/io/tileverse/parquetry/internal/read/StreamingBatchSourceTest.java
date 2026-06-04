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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.tileverse.parquetry.batch.ParquetRecordBatch;

class StreamingBatchSourceTest {

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void speculativeProducerReservesAndReleasesOnClose() throws InterruptedException {
        // Comfortably above the producer's initial per-batch estimate, which leaves headroom for all three batches.
        DecodeBudget budget = DecodeBudget.ofBytes(64L * 1024 * 1024);
        List<ParquetRecordBatch> batches =
                List.of(TestBatches.intBatch(8), TestBatches.intBatch(8), TestBatches.intBatch(8));
        BatchHandoff handoff = new BatchHandoff(2);
        StreamingBatchSource source =
                new StreamingBatchSource(handoff, new ListRowGroupDriver(batches), budget, /*exempt*/ false);

        Thread producer = startProducer(source);

        List<ParquetRecordBatch> drained = drainAll(source);
        assertThat(drained)
                .as("all speculatively decoded batches are delivered")
                .hasSize(3);

        drained.forEach(ParquetRecordBatch::close);
        producer.join();

        assertThat(budget.available())
                .as("every batch reservation is released once the batch is closed")
                .isEqualTo(budget.capacity());
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    @SuppressWarnings("java:S2925") // sleep lets the producer park on the over-budget reservation before promotion
    void promotedProducerStopsReserving() throws InterruptedException {
        DecodeBudget budget = DecodeBudget.ofBytes(1); // too small for any batch
        List<ParquetRecordBatch> batches = List.of(TestBatches.intBatch(8), TestBatches.intBatch(8));
        BatchHandoff handoff = new BatchHandoff(2);
        StreamingBatchSource source =
                new StreamingBatchSource(handoff, new ListRowGroupDriver(batches), budget, /*exempt*/ false);

        Thread producer = startProducer(source);

        Thread.sleep(100); // let the producer park on the over-budget reservation
        source.promote();

        List<ParquetRecordBatch> drained = drainAll(source);
        assertThat(drained)
                .as("promotion lets the over-budget producer finish and deliver both batches")
                .hasSize(2);

        drained.forEach(ParquetRecordBatch::close);
        producer.join();
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void speculativeProducerReservesBeforeDecoding() throws InterruptedException {
        // Batches larger than the producer's initial estimate. Reconciliation refines the estimate up to the actual
        // size; the budget then holds exactly one resident batch at a time.
        long initialEstimate = 16L * 1024 * 1024;
        long batchBytes = initialEstimate + (8L * 1024 * 1024);
        List<ParquetRecordBatch> batches = List.of(
                TestBatches.intBatchOfAtLeastBytes(batchBytes),
                TestBatches.intBatchOfAtLeastBytes(batchBytes),
                TestBatches.intBatchOfAtLeastBytes(batchBytes));
        long actualBatchBytes = batches.get(0).approximateHeapBytes();

        // Holds the initial estimate plus one resident batch, but not a second batch's reservation: once batch 0 is
        // resident, the producer must park reserving batch 1 until batch 0 is drained and closed.
        DecodeBudget budget = DecodeBudget.ofBytes(initialEstimate + actualBatchBytes);

        AtomicInteger decodeCount = new AtomicInteger();
        BatchHandoff handoff = new BatchHandoff(2);
        StreamingBatchSource source =
                new StreamingBatchSource(handoff, new CountingDriver(batches, decodeCount), budget, /*exempt*/ false);

        Thread producer = startProducer(source);

        ParquetRecordBatch first = nextWithin(source);
        assertThat(first).as("batch 0 is delivered to the consumer").isNotNull();
        awaitDecodeCount(decodeCount, 1);

        assertThat(staysAt(decodeCount, 1))
                .as("the producer does not decode batch 1 while batch 0 is held and the budget is full")
                .isTrue();

        first.close();
        awaitDecodeCount(decodeCount, 2);

        // Drain the remaining batches one at a time, closing each before pulling the next: the one-batch budget only
        // lets the producer decode the next batch once the previous one's reservation is released on close.
        int remaining = drainClosingEach(source);
        assertThat(remaining)
                .as("once headroom frees, the remaining batches stream through")
                .isEqualTo(2);

        producer.join();

        assertThat(budget.available())
                .as("every reservation is released after all batches are closed")
                .isEqualTo(budget.capacity());
    }

    private static ParquetRecordBatch nextWithin(StreamingBatchSource source) {
        assertThat(source.hasNext())
                .as("the producer delivers at least one batch")
                .isTrue();
        return source.next();
    }

    @SuppressWarnings("java:S2925") // polling under @Timeout; no condition variable spans the producer/consumer split
    private static void awaitDecodeCount(AtomicInteger decodeCount, int target) throws InterruptedException {
        while (decodeCount.get() < target) {
            Thread.sleep(10);
        }
    }

    @SuppressWarnings("java:S2925") // a fixed observation window confirms the producer parks rather than racing ahead
    private static boolean staysAt(AtomicInteger decodeCount, int expected) throws InterruptedException {
        for (int i = 0; i < 20; i++) {
            if (decodeCount.get() != expected) {
                return false;
            }
            Thread.sleep(10);
        }
        return decodeCount.get() == expected;
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

    private static int drainClosingEach(StreamingBatchSource source) {
        int count = 0;
        while (source.hasNext()) {
            ParquetRecordBatch batch = source.next();
            batch.close();
            count++;
        }
        return count;
    }

    /** Replays a fixed list of batches while counting decodes, letting a test observe when the producer allocates. */
    private static final class CountingDriver implements RowGroupBatchDriver {

        private final List<ParquetRecordBatch> batches;
        private final AtomicInteger decodeCount;
        private int index;

        CountingDriver(List<ParquetRecordBatch> batches, AtomicInteger decodeCount) {
            this.batches = List.copyOf(batches);
            this.decodeCount = decodeCount;
        }

        @Override
        public boolean hasMore() {
            return index < batches.size();
        }

        @Override
        public ParquetRecordBatch nextBatch() {
            ParquetRecordBatch batch = batches.get(index++);
            decodeCount.incrementAndGet();
            return batch;
        }

        @Override
        public void close() {
            while (index < batches.size()) {
                batches.get(index++).close();
            }
        }
    }
}
