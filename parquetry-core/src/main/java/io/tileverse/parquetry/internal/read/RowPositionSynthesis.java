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

import java.util.List;

import io.tileverse.parquetry.schema.ColumnPath;

/**
 * The inputs a row group needs to synthesize its absolute row-position column: the row group's {@code base} file row
 * offset (the sum of {@code num_rows} of every prior row group, pruned ones included) and the {@code columns} the
 * caller named for it. The synthesized values are identical for every named column; the reader produces one position
 * vector per row group and exposes it under each path. {@code base} is per row group; {@code columns} are the same for
 * every row group of one read.
 */
public record RowPositionSynthesis(long base, List<ColumnPath> columns) {

    public RowPositionSynthesis {
        columns = List.copyOf(columns);
    }
}
