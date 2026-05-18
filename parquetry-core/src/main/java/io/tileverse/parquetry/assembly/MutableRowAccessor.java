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
package io.tileverse.parquetry.assembly;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Mutable internal {@link RowAccessor} used by {@link RecordAssembler} while building a row.
 *
 * <p>Once construction is complete the assembler hands this off as a {@link RowAccessor}; callers must not mutate it
 * after that point.
 */
final class MutableRowAccessor implements RowAccessor {

    final Map<ColumnPath, Object> values = new HashMap<>();
    final Set<ColumnPath> nullGroups = new HashSet<>();

    @Override
    public Object get(ColumnPath path) {
        return values.get(path);
    }

    @Override
    public boolean isGroupNull(ColumnPath path) {
        return nullGroups.contains(path);
    }

    @Override
    public Map<ColumnPath, Object> values() {
        return Map.copyOf(values);
    }
}
