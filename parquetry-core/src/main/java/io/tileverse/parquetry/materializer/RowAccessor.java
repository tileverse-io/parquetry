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
package io.tileverse.parquetry.materializer;

import java.util.Map;

import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Read access to an assembled row's leaf values, keyed by column path.
 *
 * <p>Values are boxed primitives ({@code Integer}, {@code Long}, {@code Float}, {@code Double}, {@code Boolean}) or a
 * read-only {@code ByteBuffer} for binary/INT96 columns. A {@code null} entry means the leaf was null at some ancestor
 * or at the leaf itself.
 *
 * <p>Group-valued columns are not represented directly. Whether a group is null vs. having all-null children is
 * preserved by {@link #isGroupNull(ColumnPath)}.
 */
public interface RowAccessor {

    /**
     * Returns the value at {@code path}, or {@code null} if the leaf was null or was not included in the projection.
     */
    Object get(ColumnPath path);

    /**
     * Returns {@code true} if the group at {@code path} was null in the source row (as opposed to simply having
     * all-null children).
     */
    boolean isGroupNull(ColumnPath path);

    /** Returns an immutable snapshot of all leaf values present in this row. */
    Map<ColumnPath, Object> values();
}
