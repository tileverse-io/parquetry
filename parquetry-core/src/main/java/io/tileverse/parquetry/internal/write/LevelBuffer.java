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
package io.tileverse.parquetry.internal.write;

import java.util.Arrays;

import io.tileverse.parquetry.internal.write.page.PageEncodeJob;

/**
 * Per-page repetition/definition level buffer. Holds an {@code int[]} that grows as cells are appended;
 * {@link #snapshot(int)} returns a defensive copy sized to the cell count, suitable for handing to
 * {@link PageEncodeJob}.
 */
final class LevelBuffer {

    private int[] levels = new int[1024];
    private int size;

    void add(int level) {
        if (size == levels.length) {
            levels = Arrays.copyOf(levels, levels.length * 2);
        }
        levels[size++] = level;
    }

    int[] snapshot(int count) {
        return Arrays.copyOf(levels, count);
    }

    void clear() {
        size = 0;
    }
}
