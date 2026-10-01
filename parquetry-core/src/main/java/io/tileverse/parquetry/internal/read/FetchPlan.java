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

import java.util.List;

import lombok.NonNull;

/**
 * One row group's byte ranges to read: exactly the bytes named by the plan, with no hole bridged. Built by
 * {@link RowGroupFetcher#planFor(RowGroupSurvivor, java.util.Optional)}, the only producer, which orders the units by
 * file offset. The fetch relies on that order both to lay the units out in one buffer and to keep each column's runs in
 * data-page ordinal order.
 *
 * @param units the ranges to read, ordered by file offset
 */
public record FetchPlan(@NonNull List<FetchUnit> units) {

    public FetchPlan {
        units = List.copyOf(units);
    }

    /** The bytes requested by the plan: the size of the single pooled buffer borrowed by one fetch. */
    public long requestedBytes() {
        long total = 0;
        for (FetchUnit unit : units) {
            total += unit.length();
        }
        return total;
    }

    /**
     * First to last requested byte, holes included: the ceiling on the scratch memory held by a byte source while it
     * serves the plan, and the amount reserved against the fetch budget by a speculative prefetch.
     */
    public long spanBytes() {
        if (units.isEmpty()) {
            return 0;
        }
        long lowest = Long.MAX_VALUE;
        long highest = Long.MIN_VALUE;
        for (FetchUnit unit : units) {
            lowest = Math.min(lowest, unit.fileOffset());
            highest = Math.max(highest, unit.fileOffset() + unit.length());
        }
        return highest - lowest;
    }
}
