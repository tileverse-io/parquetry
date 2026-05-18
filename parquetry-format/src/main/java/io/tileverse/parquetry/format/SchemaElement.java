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
package io.tileverse.parquetry.format;

import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.format.enums.ConvertedType;
import io.tileverse.parquetry.format.enums.FieldRepetitionType;
import io.tileverse.parquetry.format.enums.Type;
import io.tileverse.parquetry.format.logical.LogicalType;

/**
 * One node in the {@link FileMetaData#schema()} list.
 *
 * <p>The list is a depth-first flattening of the schema tree. Group nodes have {@code type=Optional.empty()} and
 * {@code numChildren > 0}; primitive (leaf) nodes have {@code type=Optional.of(...)} and
 * {@code numChildren=OptionalInt.empty()}.
 */
public record SchemaElement(
        Optional<Type> type,
        OptionalInt typeLength,
        Optional<FieldRepetitionType> repetitionType,
        String name,
        OptionalInt numChildren,
        Optional<ConvertedType> convertedType,
        OptionalInt scale,
        OptionalInt precision,
        Optional<LogicalType> logicalType,
        OptionalInt fieldId) {}
