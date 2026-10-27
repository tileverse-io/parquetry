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
 * The GeoParquet 1.1 rules for a two-dimensional bounding box column, applied to a top-level {@code bbox} struct. The
 * struct has the fields {@code xmin}, {@code ymin}, {@code xmax} and {@code ymax} and no other. The fields are all
 * FLOAT or all DOUBLE, and the struct has the repetition of its geometry column.
 *
 * <p>The spec also requires that field order. DuckDB writes the fields as {@code xmin}, {@code xmax}, {@code ymin},
 * {@code ymax}. A writer can still declare such a struct as a covering after {@link #inSpecOrder(ParquetSchema)}
 * reorders its fields.
 */
public final class BoundingBoxColumn {

    /** The name of the bounding box column, at the root of the schema. */
    public static final String NAME = "bbox";

    /** The fields of a two-dimensional bounding box column, in the order required by the spec. */
    public static final List<String> FIELDS = List.of("xmin", "ymin", "xmax", "ymax");

    private BoundingBoxColumn() {}

    /**
     * Why the {@code bbox} column of {@code schema} cannot be the bounding box column of {@code geometryColumn}, field
     * order aside; empty when it can. Both columns must exist in {@code schema}.
     */
    public static Optional<String> defect(ParquetSchema schema, ColumnPath geometryColumn) {
        SchemaNode bbox = schema.find(ColumnPath.of(NAME)).orElseThrow();
        if (!(bbox instanceof SchemaNode.Group group)) {
            return Optional.of("the 'bbox' column is not a struct");
        }
        Optional<String> fieldDefect = fieldDefect(group);
        if (fieldDefect.isPresent()) {
            return fieldDefect;
        }
        SchemaNode geometry = schema.find(geometryColumn).orElseThrow();
        return repetitionDefect(group, geometry, geometryColumn);
    }

    private static Optional<String> fieldDefect(SchemaNode.Group bbox) {
        Map<String, SchemaNode> fields = fieldsByName(bbox);
        for (String name : FIELDS) {
            if (!fields.containsKey(name)) {
                return Optional.of("the 'bbox' column has no field '" + name + "'");
            }
        }
        for (String name : fields.keySet()) {
            if (!FIELDS.contains(name)) {
                return Optional.of("the 'bbox' column has a field '" + name + "' besides xmin, ymin, xmax and ymax");
            }
        }
        return kindDefect(fields);
    }

    private static Optional<String> kindDefect(Map<String, SchemaNode> fields) {
        Set<PrimitiveKind> kinds = new HashSet<>();
        for (String name : FIELDS) {
            Optional<PrimitiveKind> kind = floatingPointKind(fields.get(name));
            if (kind.isEmpty()) {
                return Optional.of("the field 'bbox." + name + "' is not FLOAT or DOUBLE");
            }
            kinds.add(kind.orElseThrow());
        }
        if (kinds.size() > 1) {
            return Optional.of("the fields of the 'bbox' column mix FLOAT and DOUBLE");
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
            SchemaNode.Group bbox, SchemaNode geometry, ColumnPath geometryColumn) {
        if (bbox.repetition() == geometry.repetition()) {
            return Optional.empty();
        }
        return Optional.of("the 'bbox' column is " + bbox.repetition() + " but the geometry column '"
                + geometryColumn.dot() + "' is " + geometry.repetition());
    }

    /**
     * {@code schema} with the fields of its {@code bbox} struct in the order required by the spec. Nothing else
     * changes. The struct must have no {@link #defect(ParquetSchema, ColumnPath) defect}.
     */
    public static ParquetSchema inSpecOrder(ParquetSchema schema) {
        SchemaNode.Group root = schema.root();
        List<SchemaNode> columns = new ArrayList<>(root.children());
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i) instanceof SchemaNode.Group group && NAME.equals(group.name())) {
                columns.set(i, withFieldsInSpecOrder(group));
            }
        }
        SchemaNode.Group reordered =
                new SchemaNode.Group(root.name(), root.repetition(), columns, root.logicalType(), root.fieldId());
        return new ParquetSchema(reordered);
    }

    private static SchemaNode.Group withFieldsInSpecOrder(SchemaNode.Group bbox) {
        Map<String, SchemaNode> fields = fieldsByName(bbox);
        List<SchemaNode> ordered = FIELDS.stream().map(fields::get).toList();
        return new SchemaNode.Group(bbox.name(), bbox.repetition(), ordered, bbox.logicalType(), bbox.fieldId());
    }

    private static Map<String, SchemaNode> fieldsByName(SchemaNode.Group bbox) {
        Map<String, SchemaNode> fields = new HashMap<>();
        for (SchemaNode field : bbox.children()) {
            fields.put(field.name(), field);
        }
        return fields;
    }
}
