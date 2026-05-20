/*
 * Copyright (c) 2026 Tileverse.io
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

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.format.LogicalType;

/**
 * A node in a Parquet schema tree.
 *
 * <p>Exactly two cases:
 *
 * <ul>
 *   <li>{@link Primitive} - a leaf column with a physical type and optional logical annotation.
 *   <li>{@link Group} - an interior node containing child fields.
 * </ul>
 */
public sealed interface Field permits Field.Primitive, Field.Group {

    String name();

    Repetition repetition();

    Optional<LogicalType> logicalType();

    int fieldId();

    /**
     * A leaf column with a physical encoding type and an optional logical type annotation.
     *
     * @param typeLength byte length for {@link PrimitiveKind#FIXED_LEN_BYTE_ARRAY}; empty otherwise.
     */
    record Primitive(
            String name,
            Repetition repetition,
            PrimitiveKind kind,
            OptionalInt typeLength,
            Optional<LogicalType> logicalType,
            int fieldId)
            implements Field {}

    /**
     * An interior node grouping child fields.
     *
     * <p>The children list is defensively copied and immutable after construction.
     */
    record Group(
            String name, Repetition repetition, List<Field> children, Optional<LogicalType> logicalType, int fieldId)
            implements Field {

        public Group {
            children = List.copyOf(children);
        }
    }
}
