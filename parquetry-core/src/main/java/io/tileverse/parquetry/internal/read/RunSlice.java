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

/**
 * Where one run of a column's data pages lives inside a {@link CoalescedRange}: which range, the offset and length
 * within it, and the offset-index ordinal of the run's first data page.
 */
record RunSlice(int rangeIndex, int offsetWithinRange, int length, int firstPageOrdinal) {

    RunSlice {
        if (rangeIndex < 0) {
            throw new IllegalArgumentException("rangeIndex must be >= 0, got " + rangeIndex);
        }
        if (offsetWithinRange < 0) {
            throw new IllegalArgumentException("offsetWithinRange must be >= 0, got " + offsetWithinRange);
        }
        if (length <= 0) {
            throw new IllegalArgumentException("length must be > 0, got " + length);
        }
        if (firstPageOrdinal < 0) {
            throw new IllegalArgumentException("firstPageOrdinal must be >= 0, got " + firstPageOrdinal);
        }
    }
}
