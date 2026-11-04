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
package io.tileverse.parquetry.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.format.LogicalType;

/**
 * Rewrites the lists of a schema in the standard form of the format: {@code <repetition> group <name> (LIST) { repeated
 * group list { <element> } }}.
 *
 * <p>Older writers stored lists in other shapes, and the format reads them by its backward-compatibility rules. A
 * repeated field outside a list or a map is a required list of required elements. In a list without the middle group,
 * the repeated field is the element. Each such shape is rewritten to the standard form, and a list already in three
 * levels keeps the names given by its writer.
 *
 * <p>The rewrite keeps the leaves in their order, with their repetition and definition levels: the column chunks of a
 * file stay bound to the leaves by position, and their level streams read unchanged.
 */
public final class StandardLists {

    private static final String REPEATED_GROUP_NAME = "list";
    private static final String ELEMENT_NAME = "element";
    private static final String ONE_TUPLE_NAME = "array";
    private static final String ONE_TUPLE_SUFFIX = "_tuple";
    private static final int NO_FIELD_ID = -1;

    private StandardLists() {}

    /** {@code schema} with its lists in the standard form, or {@code schema} itself when they all are. */
    public static ParquetSchema of(ParquetSchema schema) {
        SchemaNode.Group root = schema.root();
        SchemaNode.Group standardRoot = standardStruct(root);
        if (standardRoot == root) {
            return schema;
        }
        return new ParquetSchema(standardRoot);
    }

    private static SchemaNode.Group standardStruct(SchemaNode.Group struct) {
        List<SchemaNode> children = new ArrayList<>(struct.children().size());
        for (SchemaNode child : struct.children()) {
            children.add(standardField(child));
        }
        return withChildren(struct, children);
    }

    /** A field of a struct. A repeated field outside a list or a map is a list of its own type. */
    private static SchemaNode standardField(SchemaNode field) {
        if (field.repetition() == Repetition.REPEATED && !isListOrMap(field)) {
            return listOf(field);
        }
        return standardNode(field);
    }

    /** The required list of required elements read from a repeated field outside a list. */
    private static SchemaNode.Group listOf(SchemaNode repeatedField) {
        SchemaNode element = asElement(standardNode(repeatedField), NO_FIELD_ID);
        return new SchemaNode.Group(
                repeatedField.name(),
                Repetition.REQUIRED,
                List.of(repeatedGroupOf(element)),
                Optional.of(new LogicalType.ListType()),
                repeatedField.fieldId());
    }

    private static SchemaNode standardNode(SchemaNode node) {
        return switch (node) {
            case SchemaNode.Primitive primitive -> primitive;
            case SchemaNode.Group group -> standardGroup(group);
        };
    }

    private static SchemaNode.Group standardGroup(SchemaNode.Group group) {
        Optional<LogicalType> annotation = group.logicalType();
        if (annotation.isEmpty()) {
            return standardStruct(group);
        }
        return switch (annotation.get()) {
            case LogicalType.ListType _ -> standardList(group);
            case LogicalType.MapType _ -> standardMap(group);
            case LogicalType.Variant _ -> group;
            default -> standardStruct(group);
        };
    }

    /**
     * A list annotated as such. A list holding anything other than one repeated field is left as written.
     *
     * <p>The lists inside a repeated field are rewritten before it is renamed: the old writers named an inner list
     * after the stored name of its parent.
     */
    private static SchemaNode.Group standardList(SchemaNode.Group list) {
        if (list.children().size() != 1) {
            return list;
        }
        SchemaNode repeated = list.children().get(0);
        if (repeated.repetition() != Repetition.REPEATED) {
            return list;
        }
        if (isTheElement(repeated, list.name())) {
            SchemaNode element = asElement(standardNode(repeated), repeated.fieldId());
            return withChildren(list, List.of(repeatedGroupOf(element)));
        }
        SchemaNode.Group middleGroup = (SchemaNode.Group) repeated;
        SchemaNode element = middleGroup.children().get(0);
        SchemaNode.Group standardMiddleGroup = withChildren(middleGroup, List.of(standardNode(element)));
        return withChildren(list, List.of(standardMiddleGroup));
    }

    /**
     * Whether the repeated field of a list is the element itself, by the backward-compatibility rules of the format: a
     * primitive, a group of several fields, a group of one repeated field, and a group of one field named {@code array}
     * or {@code <list name>_tuple}. Otherwise the repeated field is the middle group of the standard form, and its one
     * field is the element.
     */
    private static boolean isTheElement(SchemaNode repeated, String listName) {
        return switch (repeated) {
            case SchemaNode.Primitive _ -> true;
            case SchemaNode.Group group -> !isMiddleGroup(group) || isOneTuple(group, listName);
        };
    }

    /** Whether {@code group} has the shape of the middle group of the standard form: one field, not repeated. */
    private static boolean isMiddleGroup(SchemaNode.Group group) {
        return group.children().size() == 1 && group.children().get(0).repetition() != Repetition.REPEATED;
    }

    /** Whether {@code group} is named as a struct of one field by the old writers of lists. */
    private static boolean isOneTuple(SchemaNode.Group group, String listName) {
        return group.name().equals(ONE_TUPLE_NAME) || group.name().equals(listName + ONE_TUPLE_SUFFIX);
    }

    /** A map. Its entries hold their fields as a struct does: a repeated field among them is a list. */
    private static SchemaNode.Group standardMap(SchemaNode.Group map) {
        if (map.children().size() != 1 || !(map.children().get(0) instanceof SchemaNode.Group entries)) {
            return map;
        }
        SchemaNode.Group standardEntries = standardStruct(entries);
        return withChildren(map, List.of(standardEntries));
    }

    /** {@code node} as the required element of a list. */
    private static SchemaNode asElement(SchemaNode node, int fieldId) {
        return switch (node) {
            case SchemaNode.Primitive primitive ->
                new SchemaNode.Primitive(
                        ELEMENT_NAME,
                        Repetition.REQUIRED,
                        primitive.kind(),
                        primitive.typeLength(),
                        primitive.logicalType(),
                        fieldId);
            case SchemaNode.Group group ->
                new SchemaNode.Group(ELEMENT_NAME, Repetition.REQUIRED, group.children(), group.logicalType(), fieldId);
        };
    }

    private static SchemaNode.Group repeatedGroupOf(SchemaNode element) {
        return new SchemaNode.Group(
                REPEATED_GROUP_NAME, Repetition.REPEATED, List.of(element), Optional.empty(), NO_FIELD_ID);
    }

    private static boolean isListOrMap(SchemaNode field) {
        if (!(field instanceof SchemaNode.Group group)) {
            return false;
        }
        Optional<LogicalType> annotation = group.logicalType();
        return annotation.isPresent()
                && (annotation.get() instanceof LogicalType.ListType
                        || annotation.get() instanceof LogicalType.MapType);
    }

    /** {@code group} with {@code children}, or {@code group} itself when each child is the one it holds already. */
    private static SchemaNode.Group withChildren(SchemaNode.Group group, List<SchemaNode> children) {
        if (sameNodes(group.children(), children)) {
            return group;
        }
        return new SchemaNode.Group(group.name(), group.repetition(), children, group.logicalType(), group.fieldId());
    }

    private static boolean sameNodes(List<SchemaNode> current, List<SchemaNode> candidate) {
        if (current.size() != candidate.size()) {
            return false;
        }
        for (int i = 0; i < current.size(); i++) {
            if (current.get(i) != candidate.get(i)) {
                return false;
            }
        }
        return true;
    }
}
