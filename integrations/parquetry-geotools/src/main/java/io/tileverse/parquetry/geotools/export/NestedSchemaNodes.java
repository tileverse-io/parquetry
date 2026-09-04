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
package io.tileverse.parquetry.geotools.export;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.geotools.data.nested.NestedType;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Converts a nested GeoTools {@link NestedType} into the {@link SchemaNode.Group} a
 * {@link io.tileverse.parquetry.data.ParquetRecordBatchBuilder} authors it through: the standard three-level LIST and
 * MAP encodings, and a plain struct group for a {@link NestedType.StructType}. This is the write-side mirror of
 * {@code NestedTypes} (in {@code io.tileverse.parquetry.geotools.data}), which projects those same three Parquet shapes
 * back into a {@link NestedType}.
 */
final class NestedSchemaNodes {

    private static final String LIST_REPEATED_NAME = "list";
    private static final String LIST_ELEMENT_NAME = "element";
    private static final String MAP_ENTRY_NAME = "key_value";
    private static final String MAP_KEY_NAME = "key";
    private static final String MAP_VALUE_NAME = "value";

    private NestedSchemaNodes() {}

    /** Converts {@code type} into the group node named {@code name}. */
    static SchemaNode.Group toGroup(String name, NestedType type) {
        return switch (type) {
            case NestedType.ListType list -> listGroup(name, list.element());
            case NestedType.MapType map -> mapGroup(name, map.key(), map.value());
            case NestedType.StructType struct -> structGroup(name, struct.fields());
            case NestedType.ScalarType _ -> throw new IllegalArgumentException("scalar has no group form: " + name);
            case NestedType.VariantType _ ->
                throw new IllegalArgumentException("Variant attributes are not writable: " + name);
        };
    }

    /** {@code optional group <name> (LIST) { repeated group list { <element>; } }}. */
    private static SchemaNode.Group listGroup(String name, NestedType elementType) {
        SchemaNode elementNode = childNode(LIST_ELEMENT_NAME, elementType);
        SchemaNode.Group repeated = new SchemaNode.Group(
                LIST_REPEATED_NAME, Repetition.REPEATED, List.of(elementNode), Optional.empty(), -1);
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
    }

    /** {@code optional group <name> (MAP) { repeated group key_value { required <k> key; optional <v> value; } }}. */
    private static SchemaNode.Group mapGroup(String name, NestedType keyType, NestedType valueType) {
        SchemaNode keyNode = requiredScalarNode(MAP_KEY_NAME, keyType);
        SchemaNode valueNode = childNode(MAP_VALUE_NAME, valueType);
        SchemaNode.Group entry = new SchemaNode.Group(
                MAP_ENTRY_NAME, Repetition.REPEATED, List.of(keyNode, valueNode), Optional.empty(), -1);
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, List.of(entry), Optional.of(new LogicalType.MapType()), -1);
    }

    /** {@code optional group <name> { <children> }}. */
    private static SchemaNode.Group structGroup(String name, List<NestedType.Field> fields) {
        List<SchemaNode> children = new ArrayList<>(fields.size());
        for (NestedType.Field field : fields) {
            children.add(childNode(field.name(), field.type()));
        }
        return new SchemaNode.Group(name, Repetition.OPTIONAL, children, Optional.empty(), -1);
    }

    /** A child node for {@code type}: an OPTIONAL scalar leaf, or a nested group built recursively. */
    private static SchemaNode childNode(String name, NestedType type) {
        if (type instanceof NestedType.ScalarType scalar) {
            return scalarLeaf(name, scalar.binding(), Repetition.OPTIONAL);
        }
        return toGroup(name, type);
    }

    /** A map key leaf: REQUIRED per the standard map encoding, and always a scalar. */
    private static SchemaNode requiredScalarNode(String name, NestedType type) {
        if (!(type instanceof NestedType.ScalarType scalar)) {
            throw new IllegalArgumentException("map key must be a scalar type, not " + type);
        }
        return scalarLeaf(name, scalar.binding(), Repetition.REQUIRED);
    }

    /**
     * The scalar leaf shape for {@code binding}: {@code String} maps to {@code BYTE_ARRAY} with a
     * {@link LogicalType.StringType} annotation, {@code byte[]} to a bare {@code BYTE_ARRAY}, and the boxed numeric
     * types to their matching physical kind with no annotation.
     */
    private static SchemaNode.Primitive scalarLeaf(String name, Class<?> binding, Repetition repetition) {
        PrimitiveKind kind = scalarKind(name, binding);
        Optional<LogicalType> logicalType =
                binding == String.class ? Optional.of(new LogicalType.StringType()) : Optional.empty();
        return new SchemaNode.Primitive(name, repetition, kind, OptionalInt.empty(), logicalType, -1);
    }

    private static PrimitiveKind scalarKind(String name, Class<?> binding) {
        if (binding == String.class || binding == byte[].class) {
            return PrimitiveKind.BYTE_ARRAY;
        }
        if (binding == Integer.class) {
            return PrimitiveKind.INT32;
        }
        if (binding == Long.class) {
            return PrimitiveKind.INT64;
        }
        if (binding == Float.class) {
            return PrimitiveKind.FLOAT;
        }
        if (binding == Double.class) {
            return PrimitiveKind.DOUBLE;
        }
        if (binding == Boolean.class) {
            return PrimitiveKind.BOOLEAN;
        }
        throw new IllegalArgumentException(
                "attribute '" + name + "' has an unsupported nested scalar binding " + binding.getName());
    }
}
