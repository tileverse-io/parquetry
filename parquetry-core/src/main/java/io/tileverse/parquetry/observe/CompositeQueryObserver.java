/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import io.tileverse.parquetry.filter.explain.PruningDecision;

final class CompositeQueryObserver implements QueryObserver {

    private final QueryObserver[] observers;

    CompositeQueryObserver(QueryObserver[] observers) {
        this.observers = observers.clone();
    }

    @Override
    public boolean wantsTimings() {
        for (QueryObserver observer : observers) {
            if (observer.wantsTimings()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void onQueryStarted(QueryStarted event) {
        for (QueryObserver observer : observers) {
            observer.onQueryStarted(event);
        }
    }

    @Override
    public void onRowGroupPlanned(int rowGroup, PruningDecision decision) {
        for (QueryObserver observer : observers) {
            observer.onRowGroupPlanned(rowGroup, decision);
        }
    }

    @Override
    public void onRowGroupRead(RowGroupRead event) {
        for (QueryObserver observer : observers) {
            observer.onRowGroupRead(event);
        }
    }

    @Override
    public void onQueryFinished(QueryStats stats) {
        for (QueryObserver observer : observers) {
            observer.onQueryFinished(stats);
        }
    }
}
