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
package io.tileverse.parquetry.read;

import java.util.Map;

import io.tileverse.parquetry.materializer.RowAccessor;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Immutable {@link RowAccessor} carrying the values of one nested struct or one list-of-struct element.
 *
 * <p>Keys are stored relative to the struct's own path (i.e. only the suffix below the struct itself). Callers walking
 * a {@code List<RowAccessor>} or {@code ParquetRecord.getStruct(...)} resolve fields by their relative path.
 *
 * <p>The {@code rootPath} is retained for diagnostic / debugging contexts (e.g. exception messages); the row accessor
 * itself never combines it with the relative keys.
 */
final class SubRecordRowAccessor implements RowAccessor {

    @SuppressWarnings("unused") // retained for diagnostics; intentionally not folded into key resolution
    private final ColumnPath rootPath;

    private final Map<ColumnPath, Object> values;

    SubRecordRowAccessor(ColumnPath rootPath, Map<ColumnPath, Object> values) {
        this.rootPath = rootPath;
        this.values = Map.copyOf(values);
    }

    @Override
    public Object get(ColumnPath path) {
        return values.get(path);
    }

    @Override
    public boolean isGroupNull(ColumnPath path) {
        return false; // sub-records only exist for non-null parents; their nested null tracking is encoded in values()
    }

    @Override
    public Map<ColumnPath, Object> values() {
        return values;
    }
}
