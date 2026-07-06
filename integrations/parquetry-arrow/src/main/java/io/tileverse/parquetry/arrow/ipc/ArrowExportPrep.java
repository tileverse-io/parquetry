/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.arrow.ipc;

import java.util.LinkedHashMap;
import java.util.Map;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.LevelListVector;
import io.tileverse.parquetry.columnar.LevelMapVector;
import io.tileverse.parquetry.columnar.ListVector;
import io.tileverse.parquetry.columnar.MapVector;
import io.tileverse.parquetry.columnar.ShreddedVariantVector;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.columnar.VariantVector;
import io.tileverse.parquetry.internal.read.LevelVectorAssembler;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchemaException;

/**
 * Prepares a column vector tree for Arrow export, recursing in step with its {@link ArrowField}. Each leaf is rewritten
 * to the column whose bytes match the chosen Arrow type: a dictionary leaf consolidates, an INT96 leaf becomes a
 * microsecond timestamp, a DECIMAL leaf becomes a 16-byte little-endian value, and a shredded Parquet Variant becomes
 * its unshredded {@code struct<metadata, value>} form. List, struct, and map vectors are rebuilt with their children
 * prepared, which makes the transforms apply at any depth (a list of decimals, a struct with an INT96 field, a map).
 *
 * <p>This is a separate recursion from the codec's encode pass, deliberately: the Arrow-output transforms live in the
 * Arrow layer rather than in the core codec.
 */
public final class ArrowExportPrep {

    private ArrowExportPrep() {
        // utility
    }

    /** Returns a prepared copy of {@code vector} whose leaves are rewritten to their Arrow value bytes. */
    public static ColumnVector prepareForExport(ColumnVector vector, ArrowField field) {
        return switch (vector) {
            case ListVector list -> prepareList(list, field);
            case MapVector map -> prepareMap(map, field);
            case StructVector struct -> prepareStruct(struct, field);
            case VariantVector variantColumn -> prepareVariant(variantColumn, field);
            case ShreddedVariantVector shredded -> prepareVariant(shredded.toUnshredded(), field);
            case LevelListVector levelList -> prepareForExport(LevelVectorAssembler.toArrowForm(levelList), field);
            case LevelMapVector levelMap -> prepareForExport(LevelVectorAssembler.toArrowForm(levelMap), field);
            default -> ArrowValueTransform.apply(field.leaf(), vector.toConsolidated());
        };
    }

    private static ListVector prepareList(ListVector list, ArrowField field) {
        ArrowField elementField = childField(field, 0, "element");
        ColumnVector child = prepareForExport(list.child(), elementField);
        return new ListVector(list.offsets(), child, list.validity(), list.size());
    }

    private static MapVector prepareMap(MapVector map, ArrowField field) {
        ArrowField entries = childField(field, 0, "entries");
        ArrowField keyField = childField(entries, 0, "key");
        ArrowField valueField = childField(entries, 1, "value");
        ColumnVector keys = prepareForExport(map.keys(), keyField);
        ColumnVector values = prepareForExport(map.values(), valueField);
        return new MapVector(map.offsets(), keys, values, map.validity(), map.size());
    }

    /**
     * Rebuilds a struct with each child prepared. The struct's children are keyed by a single-segment column path,
     * while the {@link ArrowField} children are ordered by field; the two are paired by name rather than by position
     * because the struct's map order need not match the field order.
     */
    private static StructVector prepareStruct(StructVector struct, ArrowField field) {
        Map<String, ArrowField> fieldsByName = new LinkedHashMap<>();
        for (ArrowField child : field.children()) {
            fieldsByName.put(child.name(), child);
        }
        Map<ColumnPath, ColumnVector> prepared = new LinkedHashMap<>();
        for (Map.Entry<ColumnPath, ColumnVector> entry : struct.children().entrySet()) {
            ColumnPath path = entry.getKey();
            ArrowField childField = requireStructField(fieldsByName, path, field);
            prepared.put(path, prepareForExport(entry.getValue(), childField));
        }
        return new StructVector(prepared, struct.validity(), struct.size());
    }

    private static VariantVector prepareVariant(VariantVector variantColumn, ArrowField field) {
        ArrowField metadataField = childField(field, 0, "metadata");
        ArrowField valueField = childField(field, 1, "value");
        BinaryVector metadata = (BinaryVector) prepareForExport(variantColumn.metadataColumn(), metadataField);
        BinaryVector value = (BinaryVector) prepareForExport(variantColumn.valueColumn(), valueField);
        return new VariantVector(metadata, value, variantColumn.validity(), variantColumn.size());
    }

    /**
     * The {@code index}-th child of {@code field}, named {@code role} for the error message. A missing child means the
     * Arrow field tree and the column vector tree have drifted out of step, which is reported rather than thrown as an
     * opaque index-out-of-bounds deep in the recursion.
     */
    private static ArrowField childField(ArrowField field, int index, String role) {
        if (index >= field.children().size()) {
            throw new ParquetSchemaException("Arrow %s field '%s' has no %s child to pair with the column vector"
                    .formatted(field.kind(), field.name(), role));
        }
        return field.children().get(index);
    }

    private static ArrowField requireStructField(
            Map<String, ArrowField> fieldsByName, ColumnPath path, ArrowField struct) {
        ArrowField childField = fieldsByName.get(path.dot());
        if (childField == null) {
            throw new ParquetSchemaException("Struct vector child '%s' has no matching Arrow field in struct '%s'"
                    .formatted(path.dot(), struct.name()));
        }
        return childField;
    }
}
