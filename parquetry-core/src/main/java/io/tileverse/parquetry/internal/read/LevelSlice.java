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

import java.lang.foreign.MemorySegment;
import java.util.Objects;

import io.tileverse.parquetry.columnar.Levels;

/**
 * A {@code [from, from + length)} window over a page's rep- or def-level sequence, handed to the nested assembler
 * instead of a per-batch copy. The borrowed view is safe because the assembler reads each window entirely within the
 * {@code nextBatch} call that produced it, before the owning column reader decodes another page; a segment-backed
 * {@link Levels} reads a reader-owned scratch buffer that the next page decode reuses, which is why a window must never
 * be retained past the producing batch assembly.
 */
// borrowed window view, never compared by content nor used as a map key; default record equality is fine
@SuppressWarnings("java:S6218")
record LevelSlice(Levels levels, int from, int length) {

    LevelSlice {
        Objects.requireNonNull(levels, "levels");
        if (from < 0 || length < 0 || from + length > levels.size()) {
            throw new IndexOutOfBoundsException(
                    "window [" + from + ", " + (from + length) + ") out of bounds for size " + levels.size());
        }
    }

    /** A window covering the whole sequence; for callers that already hold an exactly-sized per-batch sequence. */
    static LevelSlice ofWhole(Levels levels) {
        return new LevelSlice(levels, 0, levels.size());
    }

    /** The level value at window position {@code i}. */
    int at(int i) {
        return levels.get(from + i);
    }

    /**
     * Writes the window's {@code length * 4} native-order bytes into {@code dst} at {@code dstOffset}, the order
     * {@link Levels#ofSegment} reads back.
     */
    void copyInto(MemorySegment dst, long dstOffset) {
        levels.copyInto(from, length, dst, dstOffset);
    }

    /** The window's level values as a fresh {@code int[]} indexed from zero. */
    int[] toArray() {
        return levels.toArray(from, length);
    }
}
