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
package io.tileverse.parquetry.columnar;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.ToIntFunction;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.LevelMaxima;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * The level constants and element resolution for one list group, computed once at construction and reused on the
 * navigation path. The row view and the level-backed element record read this tree instead of re-walking the schema:
 * every definition level, repetition level, and leaf path the navigation needs is resolved here.
 *
 * <p>The element's shape is one {@link ElementMeta}: a scalar element names the leaf its values are read from, a
 * nested-list element holds the inner list's meta, a struct element holds the struct's field tree, a Variant element
 * holds its two binary leaves, and a map element holds the map's meta. The whole tree is resolved against the set of
 * leaf paths present in the owning vector, which marks a projection-dropped field as {@link ElementMeta.Absent}.
 *
 * @param groupDefLevel the definition level at which the list container is present
 * @param elementRepLevel an entry opens an element of this list when its repetition level is at most this
 * @param elementDefLevel an opened element is a real list slot (not a phantom null/empty marker) when its definition
 *     level is at least this
 * @param structureLeaf the leaf whose level streams define this group's per-row entry windows: the first present
 *     descendant leaf, or {@code null} when the projection dropped every descendant
 * @param multiLeaf whether materializing this list's subtree reads more than the structure leaf: true when the element
 *     shape at any depth below is a struct, Variant, or map; false for a subtree that is scalar at every depth, which
 *     the row view materializes from the structure leaf alone
 * @param element the shape of this list's element and the per-shape metadata its reads need
 */
public record ListMeta(
        int groupDefLevel,
        int elementRepLevel,
        int elementDefLevel,
        ColumnPath structureLeaf,
        boolean multiLeaf,
        ElementMeta element) {

    /**
     * One struct node (a list's struct element or a struct field within one), resolved to its presence level, its
     * structure leaf, and its ordered fields.
     *
     * @param presenceDef the struct node's max definition level; an entry whose definition is below this holds a null
     *     struct
     * @param structureLeaf the first descendant leaf present in the owning vector, or {@code null} when the projection
     *     dropped every descendant
     * @param fields the struct's fields in schema child order
     */
    public record StructMeta(int presenceDef, ColumnPath structureLeaf, List<FieldMeta> fields) {
        public StructMeta {
            fields = List.copyOf(fields);
        }
    }

    /**
     * One field of a struct: its schema name and the shape its value is read through.
     *
     * @param name the field's schema name
     * @param element the field's shape and the per-shape metadata its reads need
     */
    public record FieldMeta(String name, ElementMeta element) {}

    /**
     * Builds the metadata for the list group at {@code groupPath}, resolving every nested node against the leaf paths
     * present in {@code presentLeaves}.
     */
    public static ListMeta of(
            ParquetSchema schema,
            SchemaNode.Group listGroup,
            ColumnPath groupPath,
            Set<ColumnPath> presentLeaves,
            ToIntFunction<ColumnPath> leafOrdinal) {
        ColumnPath repeatedChildPath = repeatedChildPath(listGroup, groupPath);
        LevelMaxima groupLevels = schema.maxLevels(groupPath);
        LevelMaxima childLevels = schema.maxLevels(repeatedChildPath);
        int groupDefLevel = groupLevels.maxDefinitionLevel();
        int elementRepLevel = childLevels.maxRepetitionLevel();
        int elementDefLevel = childLevels.maxDefinitionLevel();
        ColumnPath structureLeaf = firstPresentDescendantLeaf(listGroup, groupPath, presentLeaves);
        ElementMeta element = elementMeta(schema, listGroup, groupPath, presentLeaves, leafOrdinal);
        return new ListMeta(
                groupDefLevel, elementRepLevel, elementDefLevel, structureLeaf, multiLeaf(element), element);
    }

    private static ElementMeta elementMeta(
            ParquetSchema schema,
            SchemaNode.Group listGroup,
            ColumnPath groupPath,
            Set<ColumnPath> presentLeaves,
            ToIntFunction<ColumnPath> leafOrdinal) {
        SchemaNode elementNode = elementNode(listGroup);
        ColumnPath elementPath = elementPath(listGroup, groupPath);
        int presenceDef = schema.maxLevels(elementPath).maxDefinitionLevel();
        if (elementNode instanceof SchemaNode.Group elementGroup) {
            return groupElementMeta(schema, elementGroup, elementPath, presentLeaves, leafOrdinal, presenceDef);
        }
        if (!presentLeaves.contains(elementPath)) {
            return new ElementMeta.Absent(presenceDef);
        }
        return new ElementMeta.Scalar(presenceDef, elementPath, leafOrdinal.applyAsInt(elementPath));
    }

    /**
     * Whether materializing a list with this element reads more than the structure leaf. A struct, Variant, or map
     * element spans several descendant leaves; a nested list defers to its own subtree; a scalar (or fully dropped)
     * subtree reads from the structure leaf alone.
     */
    private static boolean multiLeaf(ElementMeta element) {
        return switch (element) {
            case ElementMeta.Scalar _ -> false;
            case ElementMeta.Absent _ -> false;
            case ElementMeta.NestedList(ListMeta inner) -> inner.multiLeaf();
            case ElementMeta.Struct _ -> true;
            case ElementMeta.VariantLeaves _ -> true;
            case ElementMeta.MapEntry _ -> true;
        };
    }

    /**
     * Resolves a group node's shape by its annotation: a LIST or MAP annotation selects the collection metas, a VARIANT
     * annotation selects the two binary leaves, an unannotated repeated group reads as a legacy-shape nested list, and
     * any other group reads as a struct.
     */
    private static ElementMeta groupElementMeta(
            ParquetSchema schema,
            SchemaNode.Group group,
            ColumnPath path,
            Set<ColumnPath> presentLeaves,
            ToIntFunction<ColumnPath> leafOrdinal,
            int presenceDef) {
        Optional<LogicalType> annotation = group.logicalType();
        if (annotation.isPresent()) {
            LogicalType type = annotation.get();
            if (type instanceof LogicalType.ListType) {
                return new ElementMeta.NestedList(of(schema, group, path, presentLeaves, leafOrdinal));
            }
            if (type instanceof LogicalType.MapType) {
                return new ElementMeta.MapEntry(MapMeta.of(schema, group, path, presentLeaves, leafOrdinal));
            }
            if (type instanceof LogicalType.Variant) {
                return variantLeaves(group, path, presentLeaves, presenceDef);
            }
        }
        if (group.repetition() == Repetition.REPEATED) {
            return new ElementMeta.NestedList(of(schema, group, path, presentLeaves, leafOrdinal));
        }
        return new ElementMeta.Struct(structMeta(schema, group, path, presentLeaves, leafOrdinal));
    }

    /**
     * Builds the field tree for any group whose children are read as struct fields, resolving the structure leaf as the
     * first present descendant under the children in order. The map's {@code key_value} group reuses this: its two
     * children (key, value) read exactly like struct fields.
     */
    static StructMeta structMeta(
            ParquetSchema schema,
            SchemaNode.Group structGroup,
            ColumnPath structPath,
            Set<ColumnPath> presentLeaves,
            ToIntFunction<ColumnPath> leafOrdinal) {
        int presenceDef = schema.maxLevels(structPath).maxDefinitionLevel();
        ColumnPath structureLeaf = firstPresentDescendantLeaf(structGroup, structPath, presentLeaves);
        List<FieldMeta> fields = new ArrayList<>(structGroup.children().size());
        for (SchemaNode child : structGroup.children()) {
            fields.add(fieldMeta(schema, child, childPath(structPath, child.name()), presentLeaves, leafOrdinal));
        }
        return new StructMeta(presenceDef, structureLeaf, fields);
    }

    private static FieldMeta fieldMeta(
            ParquetSchema schema,
            SchemaNode field,
            ColumnPath fieldPath,
            Set<ColumnPath> presentLeaves,
            ToIntFunction<ColumnPath> leafOrdinal) {
        int presenceDef = schema.maxLevels(fieldPath).maxDefinitionLevel();
        if (field instanceof SchemaNode.Primitive) {
            if (!presentLeaves.contains(fieldPath)) {
                return new FieldMeta(field.name(), new ElementMeta.Absent(presenceDef));
            }
            return new FieldMeta(
                    field.name(), new ElementMeta.Scalar(presenceDef, fieldPath, leafOrdinal.applyAsInt(fieldPath)));
        }
        return groupFieldMeta(schema, (SchemaNode.Group) field, fieldPath, presentLeaves, leafOrdinal, presenceDef);
    }

    /**
     * A group field with no present descendant resolves as absent before any nested meta is built, except a map field:
     * the map materializer decides null-vs-empty-vs-present itself from whatever leaves remain.
     */
    private static FieldMeta groupFieldMeta(
            ParquetSchema schema,
            SchemaNode.Group group,
            ColumnPath fieldPath,
            Set<ColumnPath> presentLeaves,
            ToIntFunction<ColumnPath> leafOrdinal,
            int presenceDef) {
        if (!isMapAnnotated(group) && !hasPresentDescendant(group, fieldPath, presentLeaves)) {
            return new FieldMeta(group.name(), new ElementMeta.Absent(presenceDef));
        }
        ElementMeta element = groupElementMeta(schema, group, fieldPath, presentLeaves, leafOrdinal, presenceDef);
        return new FieldMeta(group.name(), element);
    }

    private static boolean isMapAnnotated(SchemaNode.Group group) {
        Optional<LogicalType> annotation = group.logicalType();
        return annotation.isPresent() && annotation.get() instanceof LogicalType.MapType;
    }

    private static ElementMeta.VariantLeaves variantLeaves(
            SchemaNode.Group group, ColumnPath groupPath, Set<ColumnPath> presentLeaves, int presenceDef) {
        rejectShredded(group);
        ColumnPath metadataLeaf = presentChild(group, groupPath, "metadata", presentLeaves);
        ColumnPath valueLeaf = presentChild(group, groupPath, "value", presentLeaves);
        return new ElementMeta.VariantLeaves(presenceDef, metadataLeaf, valueLeaf);
    }

    /**
     * Rejects a shredded Variant element under a list or map. A shredded Variant is a Variant group that has a
     * {@code typed_value} child alongside its {@code metadata} and {@code value} leaves; the per-element level windows
     * this navigation reads materialize only the unshredded {@code {metadata, value}} pair, never the shredded
     * {@code typed_value} subtree. Top-level and struct-nested shredded Variants are supported through the eager
     * assembler, which both {@code readBatches} and the streaming row path route Variant groups through; only a Variant
     * that descends from a repeated (list or map) group reaches this navigation and has no reconstruction here.
     */
    private static void rejectShredded(SchemaNode.Group group) {
        for (SchemaNode child : group.children()) {
            if (child.name().equals("typed_value")) {
                throw new ParquetFormatException(
                        "reading a shredded Variant nested under a list or map is not supported");
            }
        }
    }

    private static ColumnPath presentChild(
            SchemaNode.Group group, ColumnPath groupPath, String name, Set<ColumnPath> presentLeaves) {
        for (SchemaNode child : group.children()) {
            if (child.name().equals(name)) {
                ColumnPath childPath = childPath(groupPath, name);
                return presentLeaves.contains(childPath) ? childPath : null;
            }
        }
        return null;
    }

    private static boolean hasPresentDescendant(
            SchemaNode.Group group, ColumnPath groupPath, Set<ColumnPath> presentLeaves) {
        return firstPresentDescendantLeaf(group, groupPath, presentLeaves) != null;
    }

    private static ColumnPath firstPresentDescendantLeaf(
            SchemaNode.Group group, ColumnPath groupPath, Set<ColumnPath> presentLeaves) {
        for (SchemaNode child : group.children()) {
            ColumnPath childPath = childPath(groupPath, child.name());
            if (child instanceof SchemaNode.Primitive) {
                if (presentLeaves.contains(childPath)) {
                    return childPath;
                }
                continue;
            }
            ColumnPath nested = firstPresentDescendantLeaf((SchemaNode.Group) child, childPath, presentLeaves);
            if (nested != null) {
                return nested;
            }
        }
        return null;
    }

    private static ColumnPath childPath(ColumnPath parent, String name) {
        return ColumnPath.parse(parent.dot() + "." + name);
    }

    // The repeated child that emits a list's elements: the inner repeated group ("list") in the standard three-level
    // shape, or the repeated element itself in the legacy two-level shape.
    private static ColumnPath repeatedChildPath(SchemaNode.Group listGroup, ColumnPath groupPath) {
        SchemaNode repeated = listGroup.children().get(0);
        return childPath(groupPath, repeated.name());
    }

    // The element type below a list. The standard three-level shape wraps the element in a repeated group (the element
    // is that group's single child); the legacy two-level shape puts the repeated element directly under the list
    // group.
    private static SchemaNode elementNode(SchemaNode.Group listGroup) {
        SchemaNode repeated = listGroup.children().get(0);
        return switch (repeated) {
            case SchemaNode.Group wrapper -> wrapper.children().get(0);
            case SchemaNode.Primitive element -> element;
        };
    }

    private static ColumnPath elementPath(SchemaNode.Group listGroup, ColumnPath groupPath) {
        SchemaNode repeated = listGroup.children().get(0);
        return switch (repeated) {
            case SchemaNode.Group wrapper ->
                childPath(
                        childPath(groupPath, wrapper.name()),
                        wrapper.children().get(0).name());
            case SchemaNode.Primitive element -> childPath(groupPath, element.name());
        };
    }
}
