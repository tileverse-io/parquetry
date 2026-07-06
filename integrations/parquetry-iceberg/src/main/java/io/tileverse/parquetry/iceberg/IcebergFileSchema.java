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
package io.tileverse.parquetry.iceberg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Per-file index of an Iceberg data file's top-level columns, keyed by Iceberg {@code field_id}.
 *
 * <p>Reconciling a data file against the table's current schema starts here: every top-level column the file holds is
 * located by its field id, with its physical path and physical type recorded so a later step can decide whether the
 * column needs renaming, promotion, or null-filling against the table schema.
 *
 * <p>Scope is the schema's top-level (direct children of the root) primitive leaves only. Iceberg field-id
 * reconciliation of nested struct / list / map members is out of scope: a nested column is read by name later, not
 * reconciled here, hence a top-level {@link SchemaNode.Group} child contributes nothing to the id map.
 *
 * <p>A column written without an Iceberg field id has {@link SchemaNode#fieldId()} equal to {@code -1}. A file with no
 * embedded ids at all resolves its leaves through the table's name mapping ({@code schema.name-mapping.default}, or the
 * implicit current-schema mapping when the property is absent); a file with any embedded id keeps its ids and the
 * mapping is ignored. {@link #hasFieldIds()} reports whether the id index resolved anything; {@link #byName} remains
 * for the reserved-lineage fallback on files where nothing resolves.
 */
final class IcebergFileSchema {

    private static final int NO_FIELD_ID = -1;

    private final Map<Integer, FileColumn> byFieldId;
    private final Map<String, FileColumn> byTopLevelName;
    private final boolean hasFieldIds;

    private IcebergFileSchema(
            Map<Integer, FileColumn> byFieldId, Map<String, FileColumn> byTopLevelName, boolean hasFieldIds) {
        this.byFieldId = Map.copyOf(byFieldId);
        this.byTopLevelName = Map.copyOf(byTopLevelName);
        this.hasFieldIds = hasFieldIds;
    }

    /** A top-level column of the file, located by its physical path and described by its physical type. */
    record FileColumn(ColumnPath path, PrimitiveKind kind, Optional<LogicalType> logicalType) {}

    /**
     * Indexes the top-level primitive leaves of {@code fileSchema} by Iceberg field id, resolving id-less leaves
     * through {@code nameMapping}.
     *
     * <p>A file where any leaf has an embedded field id is indexed by those ids alone and the mapping is ignored
     * (embedded ids win, matching Iceberg's whole-file rule); a leaf without an id in such a file is absent from the id
     * index and resolves as an absent field. A file with no embedded ids resolves every leaf through the mapping:
     * resolved leaves join the id index, unresolved leaves stay out of it and are invisible to reconciliation.
     */
    static IcebergFileSchema of(ParquetSchema fileSchema, IcebergNameMapping nameMapping) {
        List<SchemaNode.Primitive> leaves = topLevelPrimitives(fileSchema);
        Map<String, FileColumn> byTopLevelName = new HashMap<>();
        for (SchemaNode.Primitive leaf : leaves) {
            byTopLevelName.put(leaf.name(), toFileColumn(leaf));
        }
        Map<Integer, FileColumn> byFieldId =
                hasEmbeddedIds(leaves) ? indexByEmbeddedId(leaves) : indexByMappedId(leaves, nameMapping);
        return new IcebergFileSchema(byFieldId, byTopLevelName, !byFieldId.isEmpty());
    }

    /** Indexes without a name mapping: only embedded field ids resolve. Production reads pass the table's mapping. */
    static IcebergFileSchema of(ParquetSchema fileSchema) {
        return of(fileSchema, IcebergNameMapping.empty());
    }

    private static List<SchemaNode.Primitive> topLevelPrimitives(ParquetSchema fileSchema) {
        List<SchemaNode.Primitive> leaves = new ArrayList<>();
        for (SchemaNode child : fileSchema.root().children()) {
            if (child instanceof SchemaNode.Primitive leaf) {
                leaves.add(leaf);
            }
        }
        return leaves;
    }

    private static boolean hasEmbeddedIds(List<SchemaNode.Primitive> leaves) {
        for (SchemaNode.Primitive leaf : leaves) {
            if (leaf.fieldId() != NO_FIELD_ID) {
                return true;
            }
        }
        return false;
    }

    private static Map<Integer, FileColumn> indexByEmbeddedId(List<SchemaNode.Primitive> leaves) {
        Map<Integer, FileColumn> byFieldId = new HashMap<>();
        for (SchemaNode.Primitive leaf : leaves) {
            if (leaf.fieldId() != NO_FIELD_ID) {
                byFieldId.put(leaf.fieldId(), toFileColumn(leaf));
            }
        }
        return byFieldId;
    }

    private static Map<Integer, FileColumn> indexByMappedId(
            List<SchemaNode.Primitive> leaves, IcebergNameMapping nameMapping) {
        Map<Integer, FileColumn> byFieldId = new HashMap<>();
        for (SchemaNode.Primitive leaf : leaves) {
            nameMapping.idFor(leaf.name()).ifPresent(fieldId -> byFieldId.put(fieldId, toFileColumn(leaf)));
        }
        return byFieldId;
    }

    private static FileColumn toFileColumn(SchemaNode.Primitive leaf) {
        return new FileColumn(ColumnPath.of(leaf.name()), leaf.kind(), leaf.logicalType());
    }

    /**
     * Whether the id index resolved at least one column, from embedded ids or through the name mapping. When
     * {@code false}, nothing in the file can be matched by field id and the reserved-lineage lookup falls back to
     * {@link #byName}.
     */
    boolean hasFieldIds() {
        return hasFieldIds;
    }

    /**
     * The top-level columns keyed by resolved Iceberg field id (embedded or mapped); a leaf that resolves to no id is
     * excluded.
     */
    Map<Integer, FileColumn> byFieldId() {
        return byFieldId;
    }

    /** The top-level column with the given Iceberg field id, if the file holds one. */
    Optional<FileColumn> byFieldId(int fieldId) {
        return Optional.ofNullable(byFieldId.get(fieldId));
    }

    /** The top-level column with the given physical name, for the name-matching fallback. */
    Optional<FileColumn> byName(String name) {
        return Optional.ofNullable(byTopLevelName.get(name));
    }
}
