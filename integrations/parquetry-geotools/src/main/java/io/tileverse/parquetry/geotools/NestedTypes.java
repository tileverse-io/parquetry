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
package io.tileverse.parquetry.geotools;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.geotools.data.nested.NestedType;
import org.geotools.data.nested.NestedType.Field;
import org.geotools.data.nested.NestedType.ListType;
import org.geotools.data.nested.NestedType.MapType;
import org.geotools.data.nested.NestedType.ScalarType;
import org.geotools.data.nested.NestedType.StructType;
import org.locationtech.jts.geom.Geometry;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Projects a Parquet {@link SchemaNode} onto the {@link NestedType} ADT for nested GeoTools attributes.
 *
 * <p>A primitive leaf is a {@link ScalarType}; a group is a struct, list, or map. The classification is structural, not
 * name-based, mirroring {@code MultiValues}: a group whose single child is a {@code repeated} group is a {@code LIST}
 * or {@code MAP} (a {@code MAP} when the repeated group has a child named {@code key} or {@code value}), and any other
 * group is a {@code STRUCT}. The synthetic {@code list}/{@code element} and {@code key_value}/{@code key}/{@code value}
 * levels are dropped: a {@code LIST<STRUCT(...)>} projects to {@code ListType(StructType(...))}, not to a list of a
 * one-field struct.
 */
final class NestedTypes {

    private NestedTypes() {}

    /**
     * True when {@code node} maps to a nested attribute (a struct, list, or map), false for a primitive leaf the flat
     * attribute mapper handles as a scalar or geometry.
     */
    static boolean isNested(SchemaNode node) {
        return node instanceof SchemaNode.Group;
    }

    /** Classifies {@code node} into its {@link NestedType}, recursing through arbitrary nesting. */
    static NestedType of(SchemaNode node) {
        if (node instanceof SchemaNode.Primitive primitive) {
            return new ScalarType(scalarBinding(primitive));
        }
        SchemaNode.Group group = (SchemaNode.Group) node;
        SchemaNode.Group repeated = singleRepeatedChild(group);
        if (repeated != null) {
            return ofRepeated(repeated);
        }
        return structOf(group);
    }

    /**
     * The single {@code repeated} group child of {@code group}, or {@code null} when {@code group} is a plain struct.
     */
    private static SchemaNode.Group singleRepeatedChild(SchemaNode.Group group) {
        if (group.children().size() != 1) {
            return null;
        }
        SchemaNode child = group.children().get(0);
        if (child instanceof SchemaNode.Group childGroup && child.repetition() == Repetition.REPEATED) {
            return childGroup;
        }
        return null;
    }

    /** A {@code repeated} wrapper is a {@code MAP} when it holds key/value children, otherwise a {@code LIST}. */
    private static NestedType ofRepeated(SchemaNode.Group repeated) {
        if (isMapWrapper(repeated)) {
            return mapOf(repeated);
        }
        return new ListType(of(listElement(repeated)));
    }

    private static boolean isMapWrapper(SchemaNode.Group repeated) {
        for (SchemaNode child : repeated.children()) {
            String name = child.name();
            if ("key".equals(name) || "value".equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static MapType mapOf(SchemaNode.Group repeated) {
        SchemaNode keyNode = childNamed(repeated, "key");
        SchemaNode valueNode = childNamed(repeated, "value");
        return new MapType(of(keyNode), of(valueNode));
    }

    /** The element node of a {@code LIST} wrapper: its single child, dropping the synthetic {@code element} level. */
    private static SchemaNode listElement(SchemaNode.Group repeated) {
        return repeated.children().get(0);
    }

    private static StructType structOf(SchemaNode.Group group) {
        List<Field> fields = new ArrayList<>(group.children().size());
        for (SchemaNode child : group.children()) {
            fields.add(new Field(child.name(), of(child)));
        }
        return new StructType(fields);
    }

    private static SchemaNode childNamed(SchemaNode.Group group, String name) {
        for (SchemaNode child : group.children()) {
            if (child.name().equals(name)) {
                return child;
            }
        }
        throw new IllegalArgumentException("group '%s' has no child named '%s'".formatted(group.name(), name));
    }

    /**
     * The Java binding for a primitive leaf, mirroring the flat attribute mapper's rules: a GEOMETRY/GEOGRAPHY logical
     * type binds to {@link Geometry}; otherwise the physical kind drives the binding. {@code INT96} has no clean Java
     * mapping, falling back to {@link Object} rather than failing the projection.
     */
    private static Class<?> scalarBinding(SchemaNode.Primitive primitive) {
        Optional<LogicalType> logical = primitive.logicalType();
        if (isGeometryType(logical)) {
            return Geometry.class;
        }
        return physicalBinding(primitive.kind(), logical);
    }

    private static Class<?> physicalBinding(PrimitiveKind kind, Optional<LogicalType> logical) {
        boolean isString = logical.map(NestedTypes::isStringLogicalType).orElse(false);
        return switch (kind) {
            case BOOLEAN -> Boolean.class;
            case INT32 -> Integer.class;
            case INT64 -> Long.class;
            case FLOAT -> Float.class;
            case DOUBLE -> Double.class;
            case BYTE_ARRAY -> isString ? String.class : byte[].class;
            case FIXED_LEN_BYTE_ARRAY -> byte[].class;
            case INT96 -> Object.class;
        };
    }

    private static boolean isGeometryType(Optional<LogicalType> logical) {
        return logical.map(lt -> lt instanceof LogicalType.Geometry || lt instanceof LogicalType.Geography)
                .orElse(false);
    }

    private static boolean isStringLogicalType(LogicalType lt) {
        return lt instanceof LogicalType.StringType
                || lt instanceof LogicalType.EnumType
                || lt instanceof LogicalType.JsonType
                || lt instanceof LogicalType.BsonType;
    }
}
