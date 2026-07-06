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
package org.geotools.data.nested;

import java.util.List;

/**
 * The shape of a nested GeoTools attribute: a struct, list, map, scalar leaf, or the Parquet Variant logical type.
 *
 * <p>This is the type metadata stored in an {@code AttributeDescriptor}'s user data (under {@link #USER_DATA_KEY}) for
 * attributes that map a nested column ({@code STRUCT}/{@code LIST}/{@code MAP}) to a single nested feature attribute.
 *
 * <ul>
 *   <li>{@link StructType} - an ordered set of named {@link Field}s.
 *   <li>{@link ListType} - a repeated element of a single nested type.
 *   <li>{@link MapType} - a key type and a value type.
 *   <li>{@link ScalarType} - a leaf bound to a Java class (the same bindings the flat attribute mapper uses).
 *   <li>{@link VariantType} - a placeholder for the future Parquet Variant logical type.
 * </ul>
 */
public sealed interface NestedType
        permits NestedType.StructType,
                NestedType.ListType,
                NestedType.MapType,
                NestedType.ScalarType,
                NestedType.VariantType {

    /**
     * The {@code AttributeDescriptor} user-data key under which the {@link NestedType} of a nested attribute is stored.
     */
    public static final String USER_DATA_KEY = "org.geotools.data.nested.NestedType";

    /** A named member of a {@link StructType}. */
    record Field(String name, NestedType type) {}

    /** A struct: an ordered list of named fields. */
    record StructType(List<Field> fields) implements NestedType {

        public StructType {
            fields = List.copyOf(fields);
        }
    }

    /** A list: a repeated element of one nested type. */
    record ListType(NestedType element) implements NestedType {}

    /** A map: a key type and a value type. */
    record MapType(NestedType key, NestedType value) implements NestedType {}

    /** A scalar leaf bound to a Java class. */
    record ScalarType(Class<?> binding) implements NestedType {}

    /** A placeholder for the Parquet Variant logical type, pending dedicated support. */
    record VariantType() implements NestedType {}
}
