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
package io.tileverse.parquetry.schema;

/**
 * Resolved max-rep / max-def pair for a single leaf column.
 *
 * <p>Produced by {@link ParquetSchema#maxLevels(ColumnPath)} via a walk from the root down to the leaf, summing
 * repetition / definition contributions per ancestor:
 *
 * <ul>
 *   <li>REQUIRED ancestor: no contribution.
 *   <li>OPTIONAL ancestor: definition += 1.
 *   <li>REPEATED ancestor: definition += 1, repetition += 1.
 * </ul>
 *
 * <p>Page decoders consume the pair to interpret rep / def level streams in {@code DataPage} and {@code DataPageV2}
 * payloads, and the writer uses it to size rep / def buffers per column.
 *
 * @param maxRepetitionLevel maximum repetition level along the path from root to the leaf; {@code 0} for non-repeated
 *     columns
 * @param maxDefinitionLevel maximum definition level along the path; counts the number of optional / nullable ancestors
 *     plus the leaf itself if it is optional
 */
public record LevelMaxima(int maxRepetitionLevel, int maxDefinitionLevel) {

    public LevelMaxima {
        if (maxRepetitionLevel < 0) {
            throw new IllegalArgumentException("maxRepetitionLevel must be >= 0, got " + maxRepetitionLevel);
        }
        if (maxDefinitionLevel < 0) {
            throw new IllegalArgumentException("maxDefinitionLevel must be >= 0, got " + maxDefinitionLevel);
        }
    }
}
