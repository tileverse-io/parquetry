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

import io.tileverse.parquetry.schema.ColumnPath;

/**
 * One row-position column to synthesize: the {@code name} it is presented under and the {@code firstRowId} base offset
 * added to each row's within-file position. A positional-delete predicate names a {@code firstRowId} of 0 (the raw
 * within-file position); an Iceberg row-id output names the data file's {@code first_row_id}.
 */
public record RowPositionColumn(ColumnPath name, long firstRowId) {

    public RowPositionColumn {
        Objects.requireNonNull(name, "name");
    }
}
