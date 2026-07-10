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
package io.tileverse.parquetry.observe;

/**
 * Push-side write observability, mirroring {@link QueryObserver}. Callbacks fire only at write and row-group boundaries
 * with immutable aggregate payloads; no callback ever fires inside a page or value loop.
 *
 * <p>Threading: every callback fires on the writing thread. Implementations must be cheap to keep the write path free
 * of blocking work.
 */
public interface WriteObserver {

    WriteObserver NONE = new WriteObserver() {};

    default void onWriteStarted(WriteStarted event) {}

    default void onRowsWritten(long totalRows) {}

    default void onRowGroupFlushed(RowGroupFlushed event) {}

    default void onIndexesWritten(IndexesWritten event) {}

    default void onWriteFinished(WriteStats stats) {}

    static WriteObserver composite(WriteObserver... observers) {
        return new CompositeWriteObserver(observers);
    }
}
