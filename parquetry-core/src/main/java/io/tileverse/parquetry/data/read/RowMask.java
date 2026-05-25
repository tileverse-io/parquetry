/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.data.read;

import java.util.Map;

import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.schema.ColumnPath;

import lombok.NonNull;

/**
 * The decode-time row mask for one row group: the surviving rows shared by every scanned column, plus each scanned
 * column's {@link OffsetIndex}, letting the column reader map pages to row spans. Present only when the row group
 * qualifies for page-skip (every scanned column is flat and has an offset index); absent means decode in full.
 *
 * @param survivingRows the rows that survive column-index pruning, relative to the row group
 * @param offsetIndexes one offset index per scanned leaf column, keyed by column path
 */
public record RowMask(
        @NonNull RowRanges survivingRows, @NonNull Map<ColumnPath, OffsetIndex> offsetIndexes) {

    public RowMask {
        offsetIndexes = Map.copyOf(offsetIndexes);
    }
}
