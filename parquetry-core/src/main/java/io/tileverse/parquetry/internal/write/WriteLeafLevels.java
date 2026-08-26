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
package io.tileverse.parquetry.internal.write;

import java.util.Arrays;

/**
 * One leaf column's per-page repetition and definition level streams, appended in lockstep and reused across pages. A
 * stream is absent when its maximum level is zero: a required flat leaf has neither, an optional flat or struct-nested
 * leaf has definition only, and a repeated leaf has both. The backing arrays grow on demand; a reader consumes the
 * {@code [0, count())} prefix and must not retain the arrays past the page, which {@link #clear()} reuses.
 */
final class WriteLeafLevels {

    private int[] repLevels;
    private int[] defLevels;
    private int count;

    WriteLeafLevels(int maxRepetitionLevel, int maxDefinitionLevel) {
        this.repLevels = maxRepetitionLevel > 0 ? new int[1024] : null;
        this.defLevels = maxDefinitionLevel > 0 ? new int[1024] : null;
        this.count = 0;
    }

    void append(int repetitionLevel, int definitionLevel) {
        ensureCapacity();
        if (repLevels != null) {
            repLevels[count] = repetitionLevel;
        }
        if (defLevels != null) {
            defLevels[count] = definitionLevel;
        }
        count++;
    }

    int[] repetitionBacking() {
        return repLevels;
    }

    int[] definitionBacking() {
        return defLevels;
    }

    int count() {
        return count;
    }

    void clear() {
        count = 0;
    }

    private void ensureCapacity() {
        if (repLevels == null && defLevels == null) {
            return;
        }
        int capacity = capacity();
        if (count < capacity) {
            return;
        }
        int grown = capacity * 2;
        if (repLevels != null) {
            repLevels = Arrays.copyOf(repLevels, grown);
        }
        if (defLevels != null) {
            defLevels = Arrays.copyOf(defLevels, grown);
        }
    }

    private int capacity() {
        if (repLevels != null) {
            return repLevels.length;
        }
        return defLevels.length;
    }
}
