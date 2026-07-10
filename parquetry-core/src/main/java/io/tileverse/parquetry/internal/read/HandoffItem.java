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

import io.tileverse.parquetry.columnar.ParquetRecordBatch;

/**
 * One item in a {@link BatchHandoff}: a batch the producer kept in heap, or a handle to one it spilled to disk. The
 * consumer {@link #take()}s it (restoring a spilled batch to heap); an unconsumed item is {@link #discard()}ed, which
 * releases its heap or disk reservation.
 */
sealed interface HandoffItem permits HandoffItem.InHeap, HandoffItem.Spilled {

    /** Hands the batch to the consumer, restoring it from disk when it was spilled. */
    ParquetRecordBatch take();

    /** Releases an unconsumed item's reservation. */
    void discard();

    /** A batch the producer reserved in heap; its release action returns its decode-budget bytes on close. */
    record InHeap(ParquetRecordBatch batch) implements HandoffItem {

        @Override
        public ParquetRecordBatch take() {
            return batch;
        }

        @Override
        public void discard() {
            batch.close();
        }
    }

    /** A batch the producer spilled; restoring reads it back and releases its disk reservation. */
    record Spilled(SpillHandle handle, BatchSpillStore store) implements HandoffItem {

        @Override
        public ParquetRecordBatch take() {
            return store.restore(handle);
        }

        @Override
        public void discard() {
            store.releaseUnconsumed(handle);
        }
    }
}
