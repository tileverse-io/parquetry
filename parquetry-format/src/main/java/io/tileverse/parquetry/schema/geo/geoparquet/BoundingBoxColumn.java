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
package io.tileverse.parquetry.schema.geo.geoparquet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * The GeoParquet 1.1 rules for a bounding box column: a top-level struct with the fields {@code xmin}, {@code ymin},
 * {@code xmax} and {@code ymax}, and optionally the pair {@code zmin} and {@code zmax}, with no other field. The fields
 * are all FLOAT or all DOUBLE, and the struct has the repetition of its geometry column. A covering declaration names
 * the planar fields of one such struct.
 *
 * <p>The spec also fixes the field order: {@code xmin, ymin, xmax, ymax}, or {@code xmin, ymin, zmin, xmax, ymax, zmax}
 * with the z pair. DuckDB writes the fields as {@code xmin, xmax, ymin, ymax}. A writer can still declare such a struct
 * as a covering after {@link #inSpecOrder(ParquetSchema, String)} reorders its fields.
 */
public final class BoundingBoxColumn {

    /** The conventional name of the bounding box column, used by DuckDB and by the covering derived by parquetry. */
    public static final String NAME = "bbox";

    private static final List<String> PLANAR_FIELDS = List.of("xmin", "ymin", "xmax", "ymax");
    private static final List<String> SPATIAL_FIELDS = List.of("xmin", "ymin", "zmin", "xmax", "ymax", "zmax");

    private BoundingBoxColumn() {}

    /**
     * The struct named by the planar paths of {@code covering}. The spec requires each path to name a field of one
     * top-level struct, after the extremum it holds: {@code [name, "xmin"]}, {@code [name, "ymin"]}, and so on. Empty
     * when the paths name anything else.
     */
    public static Optional<String> columnNamedBy(BboxCovering covering) {
        Map<String, ColumnPath> paths = Map.of(
                "xmin", covering.xmin(),
                "ymin", covering.ymin(),
                "xmax", covering.xmax(),
                "ymax", covering.ymax());
        Set<String> structs = new HashSet<>();
        for (Map.Entry<String, ColumnPath> entry : paths.entrySet()) {
            ColumnPath path = entry.getValue();
            if (path.numParts() != 2 || !path.part(1).equals(entry.getKey())) {
                return Optional.empty();
            }
            structs.add(path.part(0));
        }
        if (structs.size() != 1) {
            return Optional.empty();
        }
        return Optional.of(structs.iterator().next());
    }

    /**
     * Why the column {@code name} of {@code schema} cannot be the bounding box column of {@code geometryColumn}, field
     * order aside; empty when it can. The geometry column must exist in {@code schema}.
     */
    public static Optional<String> defect(ParquetSchema schema, String name, ColumnPath geometryColumn) {
        Optional<SchemaNode> column = schema.find(ColumnPath.of(name));
        if (column.isEmpty()) {
            return Optional.of("there is no '" + name + "' column");
        }
        if (!(column.orElseThrow() instanceof SchemaNode.Group struct)) {
            return Optional.of(columnLabel(name) + " is not a struct");
        }
        Optional<String> fieldDefect = fieldDefect(struct);
        if (fieldDefect.isPresent()) {
            return fieldDefect;
        }
        SchemaNode geometry = schema.find(geometryColumn).orElseThrow();
        return repetitionDefect(struct, geometry, geometryColumn);
    }

    private static Optional<String> fieldDefect(SchemaNode.Group struct) {
        Map<String, SchemaNode> fields = fieldsByName(struct);
        String column = columnLabel(struct.name());
        for (String field : PLANAR_FIELDS) {
            if (!fields.containsKey(field)) {
                return Optional.of(column + " has no field '" + field + "'");
            }
        }
        if (fields.containsKey("zmin") != fields.containsKey("zmax")) {
            return Optional.of(column + " has only one of the fields 'zmin' and 'zmax'");
        }
        for (String field : fields.keySet()) {
            if (!SPATIAL_FIELDS.contains(field)) {
                return Optional.of(column + " has a field '" + field + "' besides the bounding box fields");
            }
        }
        return kindDefect(struct.name(), fields);
    }

    private static Optional<String> kindDefect(String name, Map<String, SchemaNode> fields) {
        Set<PrimitiveKind> kinds = new HashSet<>();
        for (String field : specOrder(fields)) {
            Optional<PrimitiveKind> kind = floatingPointKind(fields.get(field));
            if (kind.isEmpty()) {
                return Optional.of("the field '" + name + "." + field + "' is not FLOAT or DOUBLE");
            }
            kinds.add(kind.orElseThrow());
        }
        if (kinds.size() > 1) {
            return Optional.of("the fields of " + columnLabel(name) + " mix FLOAT and DOUBLE");
        }
        return Optional.empty();
    }

    private static Optional<PrimitiveKind> floatingPointKind(SchemaNode field) {
        if (field instanceof SchemaNode.Primitive leaf
                && (leaf.kind() == PrimitiveKind.FLOAT || leaf.kind() == PrimitiveKind.DOUBLE)) {
            return Optional.of(leaf.kind());
        }
        return Optional.empty();
    }

    /**
     * A row has a bounding box exactly when it has a geometry. An optional struct beside a required geometry column
     * breaks that rule, and a reader pruning by the covering would drop the rows without a bounding box.
     */
    private static Optional<String> repetitionDefect(
            SchemaNode.Group struct, SchemaNode geometry, ColumnPath geometryColumn) {
        if (struct.repetition() == geometry.repetition()) {
            return Optional.empty();
        }
        return Optional.of(columnLabel(struct.name()) + " is " + struct.repetition() + " but the geometry column '"
                + geometryColumn.dot() + "' is " + geometry.repetition());
    }

    private static String columnLabel(String name) {
        return "the '" + name + "' column";
    }

    /**
     * {@code schema} with the fields of its struct {@code name} in the order required by the spec. Nothing else
     * changes. The struct must have no {@link #defect(ParquetSchema, String, ColumnPath) defect}.
     */
    public static ParquetSchema inSpecOrder(ParquetSchema schema, String name) {
        SchemaNode.Group root = schema.root();
        List<SchemaNode> columns = new ArrayList<>(root.children());
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i) instanceof SchemaNode.Group struct && name.equals(struct.name())) {
                columns.set(i, withFieldsInSpecOrder(struct));
            }
        }
        SchemaNode.Group reordered =
                new SchemaNode.Group(root.name(), root.repetition(), columns, root.logicalType(), root.fieldId());
        return new ParquetSchema(reordered);
    }

    private static SchemaNode.Group withFieldsInSpecOrder(SchemaNode.Group struct) {
        Map<String, SchemaNode> fields = fieldsByName(struct);
        List<SchemaNode> ordered = specOrder(fields).stream().map(fields::get).toList();
        return new SchemaNode.Group(
                struct.name(), struct.repetition(), ordered, struct.logicalType(), struct.fieldId());
    }

    /** The field order required by the spec, with the z pair when the struct has it. */
    private static List<String> specOrder(Map<String, SchemaNode> fields) {
        if (fields.containsKey("zmin")) {
            return SPATIAL_FIELDS;
        }
        return PLANAR_FIELDS;
    }

    private static Map<String, SchemaNode> fieldsByName(SchemaNode.Group struct) {
        Map<String, SchemaNode> fields = new HashMap<>();
        for (SchemaNode field : struct.children()) {
            fields.put(field.name(), field);
        }
        return fields;
    }
}
