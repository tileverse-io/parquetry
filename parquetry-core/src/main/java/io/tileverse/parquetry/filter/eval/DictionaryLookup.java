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
package io.tileverse.parquetry.filter.eval;

import java.util.Optional;

import io.tileverse.parquetry.page.dict.Dictionary;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Resolves the loaded {@link Dictionary} for a given column path within a row group. Returns empty when the column
 * isn't dictionary-encoded or the dictionary page hasn't been loaded.
 */
@FunctionalInterface
public interface DictionaryLookup {

    @SuppressWarnings("java:S1452") // Dictionary's element type is selected at runtime from PrimitiveKind
    Optional<Dictionary<?>> get(ColumnPath path);
}
