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

import java.util.LinkedHashMap;
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
 * <p>A column written without an Iceberg field id has {@link SchemaNode#fieldId()} equal to {@code -1}. When any
 * top-level primitive leaf has no field id, or when the file holds no top-level primitive leaves at all,
 * {@link #hasFieldIds()} reports {@code false} and the caller must fall back to name matching via {@link #byName}. Any
 * real Iceberg writer emits field ids, hence the fallback is best-effort for files produced outside that contract.
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

    /** Indexes the top-level primitive leaves of {@code fileSchema} by Iceberg field id. */
    static IcebergFileSchema of(ParquetSchema fileSchema) {
        Map<Integer, FileColumn> byFieldId = new LinkedHashMap<>();
        Map<String, FileColumn> byTopLevelName = new LinkedHashMap<>();
        boolean hasFieldIds = true;
        boolean sawPrimitive = false;
        for (SchemaNode child : fileSchema.root().children()) {
            if (!(child instanceof SchemaNode.Primitive leaf)) {
                continue;
            }
            sawPrimitive = true;
            FileColumn column = toFileColumn(leaf);
            byTopLevelName.put(leaf.name(), column);
            if (leaf.fieldId() == NO_FIELD_ID) {
                hasFieldIds = false;
            } else {
                byFieldId.put(leaf.fieldId(), column);
            }
        }
        return new IcebergFileSchema(byFieldId, byTopLevelName, hasFieldIds && sawPrimitive);
    }

    private static FileColumn toFileColumn(SchemaNode.Primitive leaf) {
        return new FileColumn(ColumnPath.of(leaf.name()), leaf.kind(), leaf.logicalType());
    }

    /**
     * Whether every top-level primitive leaf has a real Iceberg field id. When {@code false}, the file cannot be
     * matched by field id and the caller must fall back to {@link #byName}.
     */
    boolean hasFieldIds() {
        return hasFieldIds;
    }

    /** The top-level columns keyed by Iceberg field id, excluding any leaf that has no field id. */
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
