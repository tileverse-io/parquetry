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
package io.tileverse.parquetry.cli.cmd;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;

import io.tileverse.parquetry.cli.UnsupportedSchemaException;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Copies {@link ParquetRecord}s into a writer's {@link ParquetRecordBatchBuilder appender}, including records that
 * contain struct (group) columns.
 *
 * <p>The per-leaf metadata a copy needs (the primitive kind and the full leaf path) is loop-invariant across rows of
 * one copy. {@link #forSchema(ParquetSchema)} precomputes it once into a tree of {@link CopyNode}s; {@link #copyInto}
 * walks the tree per row.
 *
 * <p>Null structs are handled faithfully: an OPTIONAL struct whose value is null for a given row is written as a null
 * struct (via {@link ParquetRecordBatchBuilder#setNull(ColumnPath)}) and its children are not touched, matching the
 * source exactly.
 *
 * <p>The up-front {@link #requireWritable(ParquetSchema)} guard runs once before any copy and gives the user a clean
 * error for unsupported schema elements instead of a deep write failure mid-copy.
 */
public final class RecordCopier {

    /**
     * A precomputed node in the copy plan. The plan tree mirrors the schema tree: a leaf node holds the typed copy
     * action; a group node holds the struct path and the child nodes to copy when the struct is present.
     */
    private sealed interface CopyNode permits CopyNode.Leaf, CopyNode.Struct {

        /**
         * A primitive leaf: copy from the record at {@code path} to the appender at leaf index {@code appenderIndex},
         * dispatching on {@code kind}.
         */
        record Leaf(ColumnPath path, int appenderIndex, PrimitiveKind kind) implements CopyNode {}

        /**
         * A struct group: if the struct at {@code path} is null (and it is OPTIONAL), write a null struct and skip
         * children; otherwise copy each child node.
         */
        record Struct(ColumnPath path, Repetition repetition, List<CopyNode> children) implements CopyNode {}
    }

    private final List<CopyNode> plan;

    private RecordCopier(List<CopyNode> plan) {
        this.plan = plan;
    }

    /**
     * Verifies the writer can reproduce {@code schema}. Plain primitive and struct group columns are allowed; a column
     * is rejected when it is REPEATED, a LIST, a MAP, INT96, or a Parquet Variant type. Throws
     * {@link UnsupportedSchemaException} on the first violation.
     */
    public static void requireWritable(ParquetSchema schema) {
        for (SchemaNode child : schema.root().children()) {
            requireWritableNode(child, ColumnPath.of(child.name()));
        }
    }

    private static void requireWritableNode(SchemaNode node, ColumnPath path) {
        if (node instanceof SchemaNode.Primitive prim) {
            requireWritablePrimitive(prim, path);
        } else if (node instanceof SchemaNode.Group group) {
            requireWritableGroup(group, path);
        }
    }

    private static void requireWritablePrimitive(SchemaNode.Primitive prim, ColumnPath path) {
        if (prim.repetition() == Repetition.REPEATED) {
            throw new UnsupportedSchemaException("cp does not support repeated column '" + path.dot() + "'");
        }
        if (prim.kind() == PrimitiveKind.INT96) {
            throw new UnsupportedSchemaException("cp cannot write INT96 column '" + path.dot() + "'");
        }
        if (prim.logicalType().filter(lt -> lt instanceof LogicalType.Variant).isPresent()) {
            throw new UnsupportedSchemaException("cp cannot write Variant column '" + path.dot() + "'");
        }
    }

    private static void requireWritableGroup(SchemaNode.Group group, ColumnPath path) {
        if (group.repetition() == Repetition.REPEATED) {
            throw new UnsupportedSchemaException("cp does not support repeated column '" + path.dot() + "'");
        }
        boolean isList = group.logicalType()
                .filter(lt -> lt instanceof LogicalType.ListType)
                .isPresent();
        boolean isMap = group.logicalType()
                .filter(lt -> lt instanceof LogicalType.MapType)
                .isPresent();
        boolean isVariant = group.logicalType()
                .filter(lt -> lt instanceof LogicalType.Variant)
                .isPresent();
        if (isList) {
            throw new UnsupportedSchemaException("cp does not support list column '" + path.dot() + "'");
        }
        if (isMap) {
            throw new UnsupportedSchemaException("cp does not support map column '" + path.dot() + "'");
        }
        if (isVariant) {
            throw new UnsupportedSchemaException("cp cannot write Variant column '" + path.dot() + "'");
        }
        for (SchemaNode child : group.children()) {
            ColumnPath childPath = ColumnPath.parse(path.dot() + "." + child.name());
            requireWritableNode(child, childPath);
        }
    }

    /**
     * Precomputes the copy plan for {@code writeSchema}. The plan is a list of top-level {@link CopyNode}s whose
     * structure mirrors the schema tree. Leaf indices follow {@link ParquetSchema#leafColumns()} order, which is the
     * same order the bound appender uses.
     */
    public static RecordCopier forSchema(ParquetSchema writeSchema) {
        List<ColumnPath> leafColumns = writeSchema.leafColumns();
        int[] leafIndex = {0};
        List<CopyNode> plan = new ArrayList<>();
        for (SchemaNode child : writeSchema.root().children()) {
            plan.add(buildNode(child, ColumnPath.of(child.name()), leafColumns, leafIndex));
        }
        return new RecordCopier(plan);
    }

    private static CopyNode buildNode(SchemaNode node, ColumnPath path, List<ColumnPath> leafColumns, int[] leafIndex) {
        if (node instanceof SchemaNode.Primitive prim) {
            int idx = leafIndex[0]++;
            return new CopyNode.Leaf(leafColumns.get(idx), idx, prim.kind());
        }
        SchemaNode.Group group = (SchemaNode.Group) node;
        List<CopyNode> children = new ArrayList<>();
        for (SchemaNode child : group.children()) {
            ColumnPath childPath = ColumnPath.parse(path.dot() + "." + child.name());
            children.add(buildNode(child, childPath, leafColumns, leafIndex));
        }
        return new CopyNode.Struct(path, group.repetition(), children);
    }

    /**
     * Copies one record's values into {@code appender} and closes the appended row. Callers must pre-validate the
     * schema with {@link #requireWritable(ParquetSchema)}.
     */
    public void copyInto(ParquetRecordBatchBuilder appender, ParquetRecord record) {
        for (CopyNode node : plan) {
            copyNode(appender, record, node);
        }
        appender.endRow();
    }

    private void copyNode(ParquetRecordBatchBuilder appender, ParquetRecord record, CopyNode node) {
        switch (node) {
            case CopyNode.Leaf leaf -> copyLeaf(appender, record, leaf);
            case CopyNode.Struct struct -> copyStruct(appender, record, struct);
        }
    }

    private void copyLeaf(ParquetRecordBatchBuilder appender, ParquetRecord record, CopyNode.Leaf leaf) {
        if (record.isNull(leaf.path())) {
            appender.setNull(leaf.appenderIndex());
            return;
        }
        int col = leaf.appenderIndex();
        ColumnPath path = leaf.path();
        switch (leaf.kind()) {
            case BOOLEAN -> appender.setBoolean(col, record.getBoolean(path));
            case INT32 -> appender.setInt(col, record.getInt(path));
            case INT64 -> appender.setLong(col, record.getLong(path));
            case FLOAT -> appender.setFloat(col, record.getFloat(path));
            case DOUBLE -> appender.setDouble(col, record.getDouble(path));
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> appender.setBinary(col, (MemorySegment) record.get(path));
            case INT96 -> throw new UnsupportedSchemaException("cp cannot write INT96 column '" + path.dot() + "'");
        }
    }

    private void copyStruct(ParquetRecordBatchBuilder appender, ParquetRecord record, CopyNode.Struct struct) {
        ColumnPath path = struct.path();
        if (struct.repetition() == Repetition.OPTIONAL && record.isNull(path)) {
            // Write a null struct: children are implicitly null.
            appender.setNull(path);
            return;
        }
        for (CopyNode child : struct.children()) {
            copyNode(appender, record, child);
        }
    }
}
