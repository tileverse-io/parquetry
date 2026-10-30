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

import java.util.Map;

import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.internal.filter.PageIndex;
import io.tileverse.parquetry.schema.ColumnPath;

import lombok.NonNull;

/**
 * The decode-time row mask for one row group. It holds the surviving rows, shared by the scanned columns, and the
 * {@link PageIndex} of each scanned column. The column reader maps pages to row spans through that page index. A row
 * group has a mask only when its scanned columns are flat and each has a page index. Without a mask the row group is
 * decoded in full.
 *
 * @param survivingRows the rows that survive column-index pruning, relative to the row group
 * @param pageIndexes one page index per scanned leaf column, keyed by column path
 */
public record RowMask(
        @NonNull RowRanges survivingRows, @NonNull Map<ColumnPath, PageIndex> pageIndexes) {

    public RowMask {
        pageIndexes = Map.copyOf(pageIndexes);
    }
}
