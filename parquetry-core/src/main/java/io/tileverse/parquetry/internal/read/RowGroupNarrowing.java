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

import java.util.Optional;

import io.tileverse.parquetry.filter.RowRanges;

/**
 * What one row group's spatial plan decided before the group is fetched: the whole group is dropped, or it is kept with
 * an optional narrowing to the row ranges worth reading. A kept group without ranges reads every row already selected
 * for it by the filter pipeline.
 */
// S1845: the factories dropped()/whole() intentionally mirror the DROPPED/WHOLE singletons they return.
@SuppressWarnings("java:S1845")
public sealed interface RowGroupNarrowing permits RowGroupNarrowing.Dropped, RowGroupNarrowing.Kept {

    /** The row group performs no fetch and no decode. */
    record Dropped() implements RowGroupNarrowing {}

    /** The row group is read; {@code rows} present narrows it to those ranges, relative to the row group. */
    record Kept(Optional<RowRanges> rows) implements RowGroupNarrowing {}

    Dropped DROPPED = new Dropped();
    Kept WHOLE = new Kept(Optional.empty());

    static RowGroupNarrowing dropped() {
        return DROPPED;
    }

    static RowGroupNarrowing whole() {
        return WHOLE;
    }

    static RowGroupNarrowing narrowedTo(RowRanges rows) {
        return new Kept(Optional.of(rows));
    }
}
