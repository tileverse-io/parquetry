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
package io.tileverse.parquetry.arrow.ipc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.UnsupportedFeatureException;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchemaException;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * The Arrow field for one parquetry column, as a recursive tree. A primitive leaf wraps an {@link ArrowFieldType}; a
 * list, struct, map, or Parquet Variant column holds child fields. Both the IPC schema encoder (flatbuffer
 * {@code Field} tree) and the C Data Interface schema exporter read this single tree, which keeps the two emitters in
 * agreement on names, types, children, nullability, and extension metadata.
 *
 * <p>The group classification (list / map / struct / Variant) and the descent into list elements, map key/value
 * entries, and struct fields follow the same rules the read-side assembler applies, which keeps this type tree in step
 * with the column vectors the codec encodes.
 *
 * @param name the field name
 * @param kind the Arrow type family
 * @param leaf the leaf type for a {@link Kind#PRIMITIVE} field, {@code null} for composites
 * @param children child fields for composites, empty for a primitive
 * @param extensionMetadata Arrow custom-metadata key/values (GeoArrow or Variant extension tags), possibly empty
 * @param nullable whether the Arrow field admits nulls
 */
public record ArrowField(
        String name,
        Kind kind,
        ArrowFieldType leaf,
        List<ArrowField> children,
        Map<String, String> extensionMetadata,
        boolean nullable) {

    /** The Arrow type family of a field: a primitive leaf or one of the nested kinds. */
    public enum Kind {
        PRIMITIVE,
        LIST,
        STRUCT,
        MAP,
        VARIANT
    }

    public ArrowField {
        children = List.copyOf(children);
        extensionMetadata = Map.copyOf(extensionMetadata);
    }

    /**
     * The Arrow extension tag for a Parquet Variant column, modeled as {@code struct<metadata, value>} following the
     * {@code geoarrow.wkb} precedent. A consumer that does not interpret the extension (DuckDB) reads the underlying
     * struct of two binary fields; an Arrow-native consumer can recognize the Variant from the tag.
     */
    private static final Map<String, String> VARIANT_EXTENSION = Map.of("ARROW:extension:name", "arrow.variant");

    /** Resolves the Arrow field tree for one top-level schema field, with no ancestor path above it. */
    static ArrowField of(SchemaNode node, GeoArrowFields geometry) {
        return of(node, geometry, List.of());
    }

    /**
     * Resolves the Arrow field for {@code node}, whose ancestor group names (excluding the schema root) are
     * {@code prefix}. The prefix lets a primitive leaf reconstruct the full {@link ColumnPath} the geometry resolver
     * keys its extension metadata by, matching {@code ParquetSchema.leafColumns()}.
     */
    private static ArrowField of(SchemaNode node, GeoArrowFields geometry, List<String> prefix) {
        return switch (node) {
            case SchemaNode.Primitive primitive -> primitiveField(primitive, geometry, prefix);
            case SchemaNode.Group group -> groupField(group, geometry, prefix);
        };
    }

    /**
     * Rejects an export shape neither Arrow emitter can encode: a Parquet Variant nested under a list or map. Both the
     * IPC schema encoder and the C Data Interface exporter resolve this tree through {@link LogicalColumns}, which
     * calls this method, keeping the two paths in agreement on what is exportable. A top-level or struct-nested Variant
     * is supported and passes.
     */
    void validateExportable() {
        validateExportable(false);
    }

    private void validateExportable(boolean nestedInListOrMap) {
        if (kind == Kind.VARIANT && nestedInListOrMap) {
            throw new UnsupportedFeatureException(
                    "Arrow output does not support a Parquet Variant nested under a list or map (column " + name + ")");
        }
        boolean childNestedInListOrMap = nestedInListOrMap || kind == Kind.LIST || kind == Kind.MAP;
        for (ArrowField child : children) {
            child.validateExportable(childNestedInListOrMap);
        }
    }

    private static ArrowField primitiveField(
            SchemaNode.Primitive primitive, GeoArrowFields geometry, List<String> prefix) {
        ColumnPath leafPath = ColumnPath.of(append(prefix, primitive.name()));
        return new ArrowField(
                primitive.name(),
                Kind.PRIMITIVE,
                ArrowFieldType.of(primitive),
                List.of(),
                geometry.metadataFor(leafPath),
                nullable(primitive));
    }

    private static ArrowField groupField(SchemaNode.Group group, GeoArrowFields geometry, List<String> prefix) {
        return switch (classify(group)) {
            case LIST -> listField(group, geometry, prefix);
            case MAP -> mapField(group, geometry, prefix);
            case STRUCT -> structField(group, geometry, prefix);
            case VARIANT -> variantField(group);
        };
    }

    /**
     * A list field with one child for its element. The standard three-level encoding wraps the element in a repeated
     * group whose single child is the element; the legacy two-level encoding places a repeated primitive directly under
     * the list group, in which case that primitive is the element.
     */
    private static ArrowField listField(SchemaNode.Group group, GeoArrowFields geometry, List<String> prefix) {
        List<String> childPrefix = append(prefix, group.name());
        SchemaNode repeated = group.children().get(0);
        ArrowField element =
                switch (repeated) {
                    case SchemaNode.Group wrapper ->
                        of(wrapper.children().get(0), geometry, append(childPrefix, wrapper.name()));
                    case SchemaNode.Primitive legacyElement -> of(legacyElement, geometry, childPrefix);
                };
        return new ArrowField(group.name(), Kind.LIST, null, List.of(element), Map.of(), nullable(group));
    }

    /**
     * Arrow's canonical name for a map's single child struct. A strict Arrow consumer that validates the map child name
     * rejects any other name; the Parquet {@code key_value} group name is irrelevant to the Arrow consumer.
     */
    private static final String MAP_ENTRIES_NAME = "entries";

    /**
     * A map field whose single child is a non-nullable {@code struct<key, value>} entry field named {@code entries},
     * following Arrow's {@code Map} layout. The key and value resolve from the Parquet {@code key_value} group's two
     * children; the leaf paths keep the Parquet group name so geometry metadata still keys by the physical path.
     */
    private static ArrowField mapField(SchemaNode.Group group, GeoArrowFields geometry, List<String> prefix) {
        List<String> childPrefix = append(prefix, group.name());
        SchemaNode.Group keyValue = (SchemaNode.Group) group.children().get(0);
        List<String> entryPrefix = append(childPrefix, keyValue.name());
        ArrowField key = asNonNullable(of(keyValue.children().get(0), geometry, entryPrefix));
        ArrowField value = of(keyValue.children().get(1), geometry, entryPrefix);
        ArrowField entries = new ArrowField(MAP_ENTRIES_NAME, Kind.STRUCT, null, List.of(key, value), Map.of(), false);
        return new ArrowField(group.name(), Kind.MAP, null, List.of(entries), Map.of(), nullable(group));
    }

    /** Arrow requires a map's key field to be non-nullable, independent of the Parquet key's repetition. */
    private static ArrowField asNonNullable(ArrowField field) {
        if (!field.nullable()) {
            return field;
        }
        return new ArrowField(
                field.name(), field.kind(), field.leaf(), field.children(), field.extensionMetadata(), false);
    }

    private static ArrowField structField(SchemaNode.Group group, GeoArrowFields geometry, List<String> prefix) {
        List<String> childPrefix = append(prefix, group.name());
        List<ArrowField> children = new ArrayList<>(group.children().size());
        for (SchemaNode child : group.children()) {
            children.add(of(child, geometry, childPrefix));
        }
        return new ArrowField(group.name(), Kind.STRUCT, null, children, Map.of(), nullable(group));
    }

    /**
     * A Parquet Variant field modeled as Arrow {@code struct<metadata: binary, value: binary>} with the extension tag.
     * The metadata child is always present; the value child admits nulls.
     */
    private static ArrowField variantField(SchemaNode.Group group) {
        ArrowField metadata =
                new ArrowField("metadata", Kind.PRIMITIVE, ArrowFieldType.binary(), List.of(), Map.of(), false);
        ArrowField value = new ArrowField("value", Kind.PRIMITIVE, ArrowFieldType.binary(), List.of(), Map.of(), true);
        return new ArrowField(
                group.name(), Kind.VARIANT, null, List.of(metadata, value), VARIANT_EXTENSION, nullable(group));
    }

    private static GroupKind classify(SchemaNode.Group group) {
        LogicalType annotation = group.logicalType().orElse(null);
        return switch (annotation) {
            case null -> group.repetition() == Repetition.REPEATED ? GroupKind.LIST : GroupKind.STRUCT;
            case LogicalType.Variant _ -> GroupKind.VARIANT;
            case LogicalType.ListType _ -> GroupKind.LIST;
            case LogicalType.MapType _ -> GroupKind.MAP;
            default ->
                throw new ParquetSchemaException(
                        "Group %s has an unsupported logical-type annotation: %s".formatted(group.name(), annotation));
        };
    }

    private static boolean nullable(SchemaNode node) {
        return node.repetition() != Repetition.REQUIRED;
    }

    private static List<String> append(List<String> prefix, String name) {
        List<String> next = new ArrayList<>(prefix);
        next.add(name);
        return next;
    }

    private enum GroupKind {
        LIST,
        MAP,
        STRUCT,
        VARIANT
    }
}
