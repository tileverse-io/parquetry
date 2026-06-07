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

import java.util.Objects;

/**
 * A {@code [from, from + length)} window over a page's rep- or def-level array, handed to the nested assembler instead
 * of a per-batch copy. The level array is owned by the column reader's current page, is allocated fresh per page, and
 * is never mutated after decode, and the assembler reads each window entirely within one batch assembly, which makes a
 * borrowed view safe.
 */
// borrowed window view, never compared by content nor used as a map key; default record equality is fine
@SuppressWarnings("java:S6218")
record LevelSlice(int[] levels, int from, int length) {

    LevelSlice {
        Objects.requireNonNull(levels, "levels");
        if (from < 0 || length < 0 || from + length > levels.length) {
            throw new IndexOutOfBoundsException(
                    "window [" + from + ", " + (from + length) + ") out of bounds for length " + levels.length);
        }
    }

    /** A window covering the whole array; for callers that already hold an exactly-sized per-batch level array. */
    static LevelSlice ofWhole(int[] levels) {
        return new LevelSlice(levels, 0, levels.length);
    }

    /** The level value at window position {@code i}. */
    int at(int i) {
        return levels[from + i];
    }
}
