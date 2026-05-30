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
package io.tileverse.parquetry.record;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import io.tileverse.parquetry.materializer.RowAccessor;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * A {@link RowAccessor} over a fixed, owned snapshot of {@code (path -> value)} entries. Backs a detached
 * {@link ParquetRecord}: the values it serves are self-contained copies produced by {@link Detach}, with no tie to any
 * batch. Once built, it is immutable and safe to retain and share across threads.
 *
 * <p>The snapshot comes from another {@code RowAccessor}'s {@link RowAccessor#values()}, which records the leaf columns
 * present on the row. Group-level nullness is not part of that snapshot: a missing key reads as a null value, and
 * {@link #isGroupNull(ColumnPath)} reports whether the column is absent from the snapshot rather than reconstructing
 * the original group-vs-leaf distinction. For a flat row this matches the source accessor exactly; for a nested struct
 * that was null at the group level, the detached copy represents it as absent leaf entries.
 */
record MaterializedRowAccessor(Map<ColumnPath, Object> values) implements RowAccessor {

    MaterializedRowAccessor {
        values = Collections.unmodifiableMap(new LinkedHashMap<>(values));
    }

    @Override
    public Object get(ColumnPath path) {
        return values.get(path);
    }

    @Override
    public boolean isGroupNull(ColumnPath path) {
        return values.get(path) == null;
    }
}
