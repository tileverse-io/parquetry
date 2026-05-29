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
package io.tileverse.parquetry.format;

import java.util.Optional;
import java.util.OptionalInt;

/**
 * One node in the {@link FileMetaData#schema()} list.
 *
 * <p>The list is a depth-first flattening of the schema tree. Group nodes have {@code type=Optional.empty()} and
 * {@code numChildren > 0}; primitive (leaf) nodes have {@code type=Optional.of(...)} and
 * {@code numChildren=OptionalInt.empty()}.
 *
 * @param type {@link PhysicalType} for leaf nodes; empty for group nodes
 * @param typeLength byte width for {@link PhysicalType#FIXED_LEN_BYTE_ARRAY}; empty for other physical types
 * @param repetitionType {@link FieldRepetitionType} (REQUIRED / OPTIONAL / REPEATED); empty defaults to REQUIRED per
 *     the Thrift spec
 * @param name field name as written into the schema tree; never null and never empty for non-root nodes
 * @param numChildren child count for group nodes (always {@code > 0}); empty for leaf nodes
 * @param convertedType legacy {@link ConvertedType} annotation (UTF8, DECIMAL, etc.); empty when not written.
 *     Superseded by {@link #logicalType} on modern writers
 * @param scale decimal scale (digits to the right of the decimal point); empty unless the column is a DECIMAL
 * @param precision decimal precision (total digits); empty unless the column is a DECIMAL
 * @param logicalType modern {@link LogicalType} union annotation (STRING, DECIMAL, TIMESTAMP, GEOMETRY, ...); empty
 *     when the writer only recorded the legacy {@link #convertedType} or nothing
 * @param fieldId optional protobuf-style field id for schema evolution; empty when not written
 */
public record SchemaElement(
        Optional<PhysicalType> type,
        OptionalInt typeLength,
        Optional<FieldRepetitionType> repetitionType,
        String name,
        OptionalInt numChildren,
        Optional<ConvertedType> convertedType,
        OptionalInt scale,
        OptionalInt precision,
        Optional<LogicalType> logicalType,
        OptionalInt fieldId) {}
