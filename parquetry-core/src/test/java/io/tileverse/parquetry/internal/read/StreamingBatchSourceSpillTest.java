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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class StreamingBatchSourceSpillTest {

    @Test
    @Timeout(10)
    void spillsBatchesThatDoNotFitTheHeapBudgetAndStillDeliversThem(@TempDir Path dir) {
        ParquetSchema schema = singleIntColumnSchema("n");
        DecodeBudget heapBudget = DecodeBudget.ofBytes(1); // nothing fits in heap; everything spills
        DiskBudget diskBudget = DiskBudget.ofBytes(1 << 20);
        BatchSpillStore store = new BatchSpillStore(dir, diskBudget, schema);
        RowGroupBatchDriver driver =
                fakeDriver(intBatch(schema, "n", new int[] {1, 2}), intBatch(schema, "n", new int[] {3, 4, 5}));

        BatchHandoff handoff = new BatchHandoff(2);
        StreamingBatchSource source = new StreamingBatchSource(handoff, driver, heapBudget, store, true);
        ExecutorService executor = runProducerOnAThread(source);

        try {
            ParquetRecordBatch first = source.next();
            assertThat(first.rowCount()).isEqualTo(2);
            first.close();
            ParquetRecordBatch second = source.next();
            assertThat(second.rowCount()).isEqualTo(3);
            second.close();
            assertThat(source.hasNext()).isFalse();
        } finally {
            source.close();
            store.close();
            executor.shutdownNow();
        }
    }

    @Test
    @Timeout(10)
    void completesUnderTinyHeapBudgetWithoutDeadlock(@TempDir Path dir) {
        ParquetSchema schema = singleIntColumnSchema("n");
        DecodeBudget heapBudget = DecodeBudget.ofBytes(1);
        DiskBudget diskBudget = DiskBudget.ofBytes(1 << 20);
        BatchSpillStore store = new BatchSpillStore(dir, diskBudget, schema);
        ParquetRecordBatch[] batches = new ParquetRecordBatch[20];
        for (int i = 0; i < batches.length; i++) {
            batches[i] = intBatch(schema, "n", new int[] {i});
        }
        RowGroupBatchDriver driver = fakeDriver(batches);

        BatchHandoff handoff = new BatchHandoff(2);
        StreamingBatchSource source = new StreamingBatchSource(handoff, driver, heapBudget, store, true);
        ExecutorService executor = runProducerOnAThread(source);

        int count = 0;
        try {
            while (source.hasNext()) {
                ParquetRecordBatch batch = source.next();
                count++;
                batch.close();
            }
        } finally {
            source.close();
            store.close();
            executor.shutdownNow();
        }
        assertThat(count).isEqualTo(20);
    }

    @Test
    @Timeout(10)
    void cancelDuringLastResortParkDoesNotOverReleaseSharedBudget(@TempDir Path dir) throws InterruptedException {
        ParquetSchema schema = singleIntColumnSchema("n");
        // A shared budget fully held by a simulated concurrent read: zero headroom.
        DecodeBudget sharedBudget = DecodeBudget.ofBytes(100);
        assertThat(sharedBudget.tryReserve(100)).isTrue();
        DiskBudget diskBudget = DiskBudget.ofBytes(1 << 20);
        BatchSpillStore store = new BatchSpillStore(dir, diskBudget, schema);

        // The producer enters admit() with this batch, then parks in budget.reserve: spill is disabled and headroom is
        // zero. Counting the latch down inside nextBatch() lets the test cancel only after the producer is past the
        // loop's cancellation check and committed to admitting the batch, which makes the cancel/park path reliable.
        CountDownLatch enteredAdmit = new CountDownLatch(1);
        RowGroupBatchDriver driver = signallingDriver(enteredAdmit, intBatch(schema, "n", new int[] {1, 2, 3}));

        BatchHandoff handoff = new BatchHandoff(2);
        StreamingBatchSource source = new StreamingBatchSource(handoff, driver, sharedBudget, store, false);
        ExecutorService executor = runProducerOnAThread(source);

        try {
            assertThat(enteredAdmit.await(5, TimeUnit.SECONDS)).isTrue();
            // Cancel the read; the parked reserve gives up and must not attach a release for unreserved bytes.
            source.close();
            store.close();
        } finally {
            executor.shutdownNow();
        }

        // The simulated concurrent read still holds all 100 bytes; nothing was wrongly returned.
        assertThat(sharedBudget.available()).isZero();
    }

    private static RowGroupBatchDriver signallingDriver(CountDownLatch enteredAdmit, ParquetRecordBatch... batches) {
        ListRowGroupDriver delegate = new ListRowGroupDriver(List.of(batches));
        return new RowGroupBatchDriver() {
            @Override
            public boolean hasMore() {
                return delegate.hasMore();
            }

            @Override
            public ParquetRecordBatch nextBatch() {
                ParquetRecordBatch batch = delegate.nextBatch();
                enteredAdmit.countDown();
                return batch;
            }

            @Override
            public void close() {
                delegate.close();
            }
        };
    }

    private static ExecutorService runProducerOnAThread(StreamingBatchSource source) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> future = executor.submit(source::runProducer);
        source.attachProducerTask(future);
        return executor;
    }

    private static RowGroupBatchDriver fakeDriver(ParquetRecordBatch... batches) {
        return new ListRowGroupDriver(List.of(batches));
    }

    private static ParquetRecordBatch intBatch(ParquetSchema schema, String name, int[] values) {
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of(name), IntVector.materialized(values, Validity.allValid(values.length)));
        return DefaultParquetRecordBatch.ofHeap(schema, columns, values.length);
    }

    private static ParquetSchema singleIntColumnSchema(String name) {
        SchemaNode.Primitive leaf = new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root = new SchemaNode.Group("root", Repetition.REQUIRED, List.of(leaf), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
