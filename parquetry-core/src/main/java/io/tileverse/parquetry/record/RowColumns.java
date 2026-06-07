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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * The ordered column set shared by every row of one batch, and built once per struct vector for nested rows. Holds the
 * projected columns in schema order with a path index; a per-row record is then only a reference to this plus a row
 * index, which keeps positional access allocation-free.
 *
 * <p>Order comes from the group node's child order, not the (unordered) column map. A child that the projection
 * dropped, or that assembled to no vector, has a {@code null} entry in {@link #vector(int)}; accessors read it as a
 * null cell.
 */
public final class RowColumns {

    private final ParquetSchema schema;
    private final SchemaNode.Group group;
    private final ColumnPath[] paths;
    private final ColumnVector[] vectors;
    private final Map<ColumnPath, Integer> indexByPath;

    private RowColumns(
            ParquetSchema schema,
            SchemaNode.Group group,
            ColumnPath[] paths,
            ColumnVector[] vectors,
            Map<ColumnPath, Integer> indexByPath) {
        this.schema = schema;
        this.group = group;
        this.paths = paths;
        this.vectors = vectors;
        this.indexByPath = indexByPath;
    }

    /**
     * Builds an ordered view of {@code columns} keyed off {@code group}'s child order. Each child name is resolved to
     * its vector in {@code columns}; a child absent from {@code columns} maps to a {@code null} vector.
     */
    public static RowColumns of(ParquetSchema schema, SchemaNode.Group group, Map<ColumnPath, ColumnVector> columns) {
        List<SchemaNode> children = group.children();
        int count = children.size();
        ColumnPath[] paths = new ColumnPath[count];
        ColumnVector[] vectors = new ColumnVector[count];
        Map<ColumnPath, Integer> indexByPath = HashMap.newHashMap(count);
        for (int i = 0; i < count; i++) {
            ColumnPath path = ColumnPath.of(children.get(i).name());
            paths[i] = path;
            vectors[i] = columns.get(path);
            indexByPath.put(path, i);
        }
        return new RowColumns(schema, group, paths, vectors, indexByPath);
    }

    /**
     * Builds an ordered view of a struct's children, in the struct's own child order (the assembler inserts children in
     * schema order, and {@link StructVector} preserves that order). Used for struct sub-records, which navigate by the
     * struct vector alone without a schema group node.
     */
    public static RowColumns ofStruct(ParquetSchema schema, StructVector struct) {
        Map<ColumnPath, ColumnVector> children = struct.children();
        int count = children.size();
        ColumnPath[] paths = new ColumnPath[count];
        ColumnVector[] vectors = new ColumnVector[count];
        Map<ColumnPath, Integer> indexByPath = HashMap.newHashMap(count);
        int i = 0;
        for (Map.Entry<ColumnPath, ColumnVector> entry : children.entrySet()) {
            paths[i] = entry.getKey();
            vectors[i] = entry.getValue();
            indexByPath.put(entry.getKey(), i);
            i++;
        }
        return new RowColumns(schema, null, paths, vectors, indexByPath);
    }

    ParquetSchema schema() {
        return schema;
    }

    SchemaNode.Group group() {
        return group;
    }

    int columnCount() {
        return paths.length;
    }

    ColumnPath path(int col) {
        return paths[col];
    }

    ColumnVector vector(int col) {
        return vectors[col];
    }

    int indexOf(ColumnPath path) {
        Integer index = indexByPath.get(path);
        return index == null ? -1 : index;
    }
}
