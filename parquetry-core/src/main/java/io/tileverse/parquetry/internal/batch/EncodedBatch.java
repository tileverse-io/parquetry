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
package io.tileverse.parquetry.internal.batch;

import java.util.List;

import io.tileverse.parquetry.internal.arrow.buffer.EncodedNode;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * The Arrow buffer layout of one record batch as an in-memory node tree: a row count, the top-level column paths in
 * their batch order, and one {@link EncodedNode} per column at the matching position.
 *
 * <p>This is the schema-driven counterpart of the per-column {@link EncodedNode}: each top-level column of a
 * {@code ParquetRecordBatch} encodes to one node, and the batch's row count plus the ordered column list pin the nodes
 * back to their columns on decode. Holding the nodes in memory keeps the structure independent of any byte-level
 * serialization; writing the body and an index to a file is a separate concern.
 *
 * @param rowCount the batch's logical row count
 * @param columns the top-level column paths in batch order, one per node
 * @param nodes the encoded node for each column, positionally aligned with {@code columns}
 */
public record EncodedBatch(int rowCount, List<ColumnPath> columns, List<EncodedNode> nodes) {

    public EncodedBatch {
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount must be >= 0, got " + rowCount);
        }
        columns = List.copyOf(columns);
        nodes = List.copyOf(nodes);
        if (columns.size() != nodes.size()) {
            throw new IllegalArgumentException(
                    "columns and nodes must have equal size, got " + columns.size() + " and " + nodes.size());
        }
    }
}
