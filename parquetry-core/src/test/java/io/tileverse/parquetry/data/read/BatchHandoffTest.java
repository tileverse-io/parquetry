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
package io.tileverse.parquetry.data.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.Arena;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.tileverse.parquetry.batch.DefaultParquetRecordBatch;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class BatchHandoffTest {

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void deliversBatchesInOrderThenCompletes() throws InterruptedException {
        BatchHandoff handoff = new BatchHandoff(2);
        ParquetRecordBatch b0 = batch();
        ParquetRecordBatch b1 = batch();
        handoff.put(b0);
        handoff.put(b1);
        handoff.complete();

        assertThat(handoff.hasNext()).isTrue();
        assertThat(handoff.next()).isSameAs(b0);
        assertThat(handoff.next()).isSameAs(b1);
        assertThat(handoff.hasNext()).isFalse();

        b0.close();
        b1.close();
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void putBlocksWhenFullUntilConsumerDrains() throws InterruptedException {
        BatchHandoff handoff = new BatchHandoff(1);
        ParquetRecordBatch b0 = batch();
        ParquetRecordBatch b1 = batch();
        handoff.put(b0);

        CountDownLatch secondPutReturned = new CountDownLatch(1);
        Thread producer = new Thread(() -> {
            try {
                handoff.put(b1);
                secondPutReturned.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        producer.start();

        assertThat(secondPutReturned.await(200, TimeUnit.MILLISECONDS))
                .as("second put blocks while the queue is full")
                .isFalse();
        handoff.next(); // drain b0, freeing a slot
        assertThat(secondPutReturned.await(2, TimeUnit.SECONDS)).isTrue();
        producer.join();

        b0.close();
        handoff.next().close();
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void failurePropagatesToConsumer() {
        BatchHandoff handoff = new BatchHandoff(2);
        handoff.fail(new IllegalStateException("decode boom"));
        assertThatThrownBy(handoff::hasNext)
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("decode boom");
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void closeClosesUndeliveredBatches() throws InterruptedException {
        BatchHandoff handoff = new BatchHandoff(2);
        Arena arena = Arena.ofShared();
        AtomicInteger releases = new AtomicInteger();
        ParquetRecordBatch undelivered = batch(arena);
        undelivered.attachReleaseAction(releases::incrementAndGet);
        handoff.put(undelivered);
        handoff.complete();

        handoff.close();

        assertThat(releases.get()).as("undelivered batch is closed by close()").isEqualTo(1);
        assertThatThrownBy(() -> arena.allocate(8)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void cancelledPutClosesHeldBatchAndStopsProducer() throws InterruptedException {
        BatchHandoff handoff = new BatchHandoff(1);
        handoff.put(batch()); // fill the single slot

        Arena arena = Arena.ofShared();
        AtomicInteger releases = new AtomicInteger();
        ParquetRecordBatch held = batch(arena);
        held.attachReleaseAction(releases::incrementAndGet);

        CountDownLatch putReturned = new CountDownLatch(1);
        Thread producer = new Thread(() -> {
            try {
                handoff.put(held);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                putReturned.countDown();
            }
        });
        producer.start();
        Thread.sleep(100); // let the producer park on the full queue

        handoff.close(); // cancels: drains the queued batch and releases the parked producer

        assertThat(putReturned.await(2, TimeUnit.SECONDS)).isTrue();
        producer.join();
        assertThat(releases.get())
                .as("the batch held by the parked put is closed on cancel")
                .isEqualTo(1);
    }

    @Test
    void isCancelledReflectsClose() {
        BatchHandoff handoff = new BatchHandoff(1);
        assertThat(handoff.isCancelled()).isFalse();
        handoff.close();
        assertThat(handoff.isCancelled()).isTrue();
    }

    private static ParquetRecordBatch batch() {
        return batch(Arena.ofShared());
    }

    private static ParquetRecordBatch batch(Arena arena) {
        return new DefaultParquetRecordBatch(schema(), Map.of(), 0, arena);
    }

    private static ParquetSchema schema() {
        return new ParquetSchema(new SchemaNode.Group(
                "root",
                Repetition.REQUIRED,
                List.of(new SchemaNode.Primitive(
                        "value", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1)),
                Optional.empty(),
                -1));
    }
}
