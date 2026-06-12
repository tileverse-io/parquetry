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
package io.tileverse.parquetry.observe;

final class CompositeWriteObserver implements WriteObserver {

    private final WriteObserver[] observers;

    CompositeWriteObserver(WriteObserver[] observers) {
        this.observers = observers.clone();
    }

    @Override
    public void onWriteStarted(WriteStarted event) {
        for (WriteObserver observer : observers) {
            observer.onWriteStarted(event);
        }
    }

    @Override
    public void onRowsWritten(long totalRows) {
        for (WriteObserver observer : observers) {
            observer.onRowsWritten(totalRows);
        }
    }

    @Override
    public void onRowGroupFlushed(RowGroupFlushed event) {
        for (WriteObserver observer : observers) {
            observer.onRowGroupFlushed(event);
        }
    }

    @Override
    public void onIndexesWritten(IndexesWritten event) {
        for (WriteObserver observer : observers) {
            observer.onIndexesWritten(event);
        }
    }

    @Override
    public void onWriteFinished(WriteStats stats) {
        for (WriteObserver observer : observers) {
            observer.onWriteFinished(stats);
        }
    }
}
