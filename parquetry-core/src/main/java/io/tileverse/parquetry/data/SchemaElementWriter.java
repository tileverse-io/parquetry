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
package io.tileverse.parquetry.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.format.ConvertedType;
import io.tileverse.parquetry.format.FieldRepetitionType;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.SchemaElement;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Flattens a {@link ParquetSchema} tree into the depth-first {@link SchemaElement} list the Thrift footer carries.
 *
 * <p>Inverse of {@link io.tileverse.parquetry.schema.SchemaBuilder}: each group becomes one element with
 * {@code numChildren}, followed in pre-order by its children's elements; each primitive becomes one element with
 * {@code type} and, when applicable, {@code typeLength}. The root group surfaces as a node named {@code "schema"} when
 * the source schema's root is anonymous, matching the convention the parquet-format writers in the wild use.
 */
final class SchemaElementWriter {

    private SchemaElementWriter() {}

    static List<SchemaElement> flatten(ParquetSchema schema) {
        List<SchemaElement> out = new ArrayList<>();
        appendField(schema.root(), out, true);
        return out;
    }

    private static void appendField(SchemaNode field, List<SchemaElement> out, boolean isRoot) {
        switch (field) {
            case SchemaNode.Primitive primitive -> out.add(toPrimitiveElement(primitive));
            case SchemaNode.Group group -> {
                out.add(toGroupElement(group, isRoot));
                for (SchemaNode child : group.children()) {
                    appendField(child, out, false);
                }
            }
        }
    }

    private static SchemaElement toGroupElement(SchemaNode.Group group, boolean isRoot) {
        String name = isRoot && group.name().isEmpty() ? "schema" : group.name();
        Optional<FieldRepetitionType> repetition = isRoot ? Optional.empty() : Optional.of(mapRepetition(group));
        OptionalInt fieldId = group.fieldId() >= 0 ? OptionalInt.of(group.fieldId()) : OptionalInt.empty();
        return new SchemaElement(
                Optional.empty(),
                OptionalInt.empty(),
                repetition,
                name,
                OptionalInt.of(group.children().size()),
                Optional.<ConvertedType>empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                group.logicalType(),
                fieldId);
    }

    private static SchemaElement toPrimitiveElement(SchemaNode.Primitive primitive) {
        OptionalInt fieldId = primitive.fieldId() >= 0 ? OptionalInt.of(primitive.fieldId()) : OptionalInt.empty();
        return new SchemaElement(
                Optional.of(toPhysicalType(primitive.kind())),
                primitive.typeLength(),
                Optional.of(mapRepetition(primitive)),
                primitive.name(),
                OptionalInt.empty(),
                Optional.<ConvertedType>empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                primitive.logicalType(),
                fieldId);
    }

    private static FieldRepetitionType mapRepetition(SchemaNode field) {
        return switch (field.repetition()) {
            case REQUIRED -> FieldRepetitionType.REQUIRED;
            case OPTIONAL -> FieldRepetitionType.OPTIONAL;
            case REPEATED -> FieldRepetitionType.REPEATED;
        };
    }

    private static PhysicalType toPhysicalType(PrimitiveKind kind) {
        return switch (kind) {
            case BOOLEAN -> PhysicalType.BOOLEAN;
            case INT32 -> PhysicalType.INT32;
            case INT64 -> PhysicalType.INT64;
            case INT96 -> PhysicalType.INT96;
            case FLOAT -> PhysicalType.FLOAT;
            case DOUBLE -> PhysicalType.DOUBLE;
            case BYTE_ARRAY -> PhysicalType.BYTE_ARRAY;
            case FIXED_LEN_BYTE_ARRAY -> PhysicalType.FIXED_LEN_BYTE_ARRAY;
        };
    }
}
