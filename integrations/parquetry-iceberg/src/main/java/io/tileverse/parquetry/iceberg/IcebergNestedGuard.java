/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.iceberg;

import java.util.HashMap;
import java.util.Map;

import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Fails loud when a data file's nested field ids disagree with the table schema.
 *
 * <p>The reader reconciles only top-level primitives by field id; nested struct/list/map members are read by physical
 * name. When a table evolves a nested member the id stays stable while the name moves (a rename) or a name is reused
 * for a new id (a reassignment); reading by name then returns nulls where data exists or a value that by id belongs to
 * a different field. This guard walks the file footer's id-bearing nested nodes and compares them with the table's
 * nested schema in both directions, throwing an {@link IcebergFormatException} naming the offending path on the first
 * disagreement. It is detection only: it changes nothing about what is read.
 *
 * <p>One-sided nested nodes (present only in the table, or only in the file) are tolerated, matching how top-level add
 * and drop are tolerated. A nested node with no embedded field id cannot be checked and is skipped. When the table is
 * flat the guard is a no-op.
 *
 * <p>Nested members are matched by their physical Parquet name. A standard Parquet list wraps its element in a
 * synthetic {@code list} group and a map wraps its entries in a synthetic {@code key_value} group; both wrappers have
 * no field id and are skipped, and the element / key / value nodes are named {@code element} / {@code key} /
 * {@code value}, aligning with the tokens the table nested schema uses. A file that names its collection members
 * differently is a boundary this check does not cover: it may mis-flag such a file, and reconciling non-standard
 * collection naming is left to full nested reconciliation. An id-bearing member under an id-less intermediate group is
 * likewise a malformed shape the check does not special-case.
 */
final class IcebergNestedGuard {

    private static final int NO_FIELD_ID = -1;

    private IcebergNestedGuard() {}

    /** Throws {@link IcebergFormatException} when {@code file}'s nested field ids disagree with {@code table}. */
    static void validate(IcebergNestedSchema table, ParquetSchema file) {
        if (table.isEmpty()) {
            return;
        }
        Map<Integer, FileNode> fileById = new HashMap<>();
        for (SchemaNode child : file.root().children()) {
            if (child instanceof SchemaNode.Group group) {
                collectNested(group, IcebergNestedSchema.ABSENT_PARENT, fileById);
            }
        }
        for (Map.Entry<Integer, FileNode> entry : fileById.entrySet()) {
            checkNode(table, entry.getKey(), entry.getValue(), fileById);
        }
    }

    private record FileNode(int parentId, String localName) {}

    private static void collectNested(SchemaNode.Group group, int parentId, Map<Integer, FileNode> fileById) {
        if (group.fieldId() != NO_FIELD_ID) {
            fileById.put(group.fieldId(), new FileNode(parentId, group.name()));
        }
        int childParentId = group.fieldId() != NO_FIELD_ID ? group.fieldId() : parentId;
        for (SchemaNode child : group.children()) {
            switch (child) {
                case SchemaNode.Group childGroup -> collectNested(childGroup, childParentId, fileById);
                case SchemaNode.Primitive leaf
                when leaf.fieldId() != NO_FIELD_ID ->
                    fileById.put(leaf.fieldId(), new FileNode(childParentId, leaf.name()));
                default -> {
                    // An id-less leaf cannot be matched by field id and is skipped.
                }
            }
        }
    }

    private static void checkNode(
            IcebergNestedSchema table, int fileId, FileNode fileNode, Map<Integer, FileNode> fileById) {
        if (table.hasId(fileId)) {
            String tableName = table.nameOf(fileId);
            int tableParent = table.parentOf(fileId);
            if (!tableName.equals(fileNode.localName()) || tableParent != fileNode.parentId()) {
                throw new IcebergFormatException("nested field id %d: file path %s disagrees with table path %s"
                        .formatted(fileId, filePath(fileId, fileById), table.path(fileId)));
            }
        }
        if (table.hasSlot(fileNode.parentId(), fileNode.localName())) {
            int tableId = table.idAt(fileNode.parentId(), fileNode.localName());
            if (tableId != fileId) {
                throw new IcebergFormatException("nested field %s: file field id %d disagrees with table field id %d"
                        .formatted(filePath(fileId, fileById), fileId, tableId));
            }
        }
    }

    /** The dotted path to a file node, chasing parent ids to the top-level column (e.g. {@code bbox.xmin}). */
    private static String filePath(int fileId, Map<Integer, FileNode> fileById) {
        FileNode node = fileById.get(fileId);
        if (node == null) {
            return String.valueOf(fileId);
        }
        if (node.parentId() == IcebergNestedSchema.ABSENT_PARENT) {
            return node.localName();
        }
        return filePath(node.parentId(), fileById) + "." + node.localName();
    }
}
