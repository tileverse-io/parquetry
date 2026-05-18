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
package io.tileverse.parquetry.schema;

import java.util.List;

/**
 * Dot-separated path to a column within a Parquet schema tree.
 *
 * <p>The root group is not included in the path; the first element is the direct child of root.
 * For a flat schema with column "year", the path is simply {@code ColumnPath.of("year")}.
 */
public record ColumnPath(List<String> parts) {

    public ColumnPath {
        parts = List.copyOf(parts);
    }

    /** Creates a path from the given name segments. */
    public static ColumnPath of(String... parts) {
        return new ColumnPath(List.of(parts));
    }

    /** Returns the path as a dot-joined string, e.g. {@code "a.b.c"}. */
    public String dot() {
        return String.join(".", parts);
    }
}
