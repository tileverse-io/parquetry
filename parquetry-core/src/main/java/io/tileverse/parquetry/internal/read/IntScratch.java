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

/**
 * A reader-owned, grow-only, reused {@code int[]} for per-page transients (binary positions and offsets, dictionary
 * indices, dense decoder outputs). {@link #array(int)} returns the same backing array across pages while the capacity
 * suffices, growing to the next power of two when a page exceeds it. The array's contents are whatever the previous
 * page left behind beyond what the caller writes; callers that expose slots they did not write (a nullable dictionary
 * page's placeholder indexes) must fill them first. The array must never be retained past the page that filled it.
 */
final class IntScratch {

    private static final int MIN_CAPACITY = 1024;

    private int[] array = new int[0];

    /** A reused array with at least {@code minLength} slots; slots the caller does not write hold stale values. */
    int[] array(int minLength) {
        if (array.length < minLength) {
            array = new int[Math.max(MIN_CAPACITY, ceilPowerOfTwo(minLength))];
        }
        return array;
    }

    private static int ceilPowerOfTwo(int minLength) {
        return 1 << (32 - Integer.numberOfLeadingZeros(minLength - 1));
    }
}
