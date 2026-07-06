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
package io.tileverse.parquetry.format.codec;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.format.ConvertedType;
import io.tileverse.parquetry.format.FieldRepetitionType;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.SchemaElement;

/**
 * Deserializer for the Thrift {@code SchemaElement} struct.
 *
 * <pre>
 * struct SchemaElement {
 *   1: optional PhysicalType type
 *   2: optional i32 type_length
 *   3: optional FieldRepetitionType repetition_type
 *   4: required string name
 *   5: optional i32 num_children
 *   6: optional ConvertedType converted_type
 *   7: optional i32 scale
 *   8: optional i32 precision
 *   9: optional i32 field_id
 *   10: optional LogicalType logicalType
 * }
 * </pre>
 */
final class SchemaElementDeserializer {

    private SchemaElementDeserializer() {}

    static SchemaElement read(CompactProtocolReader r) throws IOException {
        Optional<PhysicalType> type = Optional.empty();
        OptionalInt typeLength = OptionalInt.empty();
        Optional<FieldRepetitionType> repetitionType = Optional.empty();
        String name = "";
        OptionalInt numChildren = OptionalInt.empty();
        Optional<ConvertedType> convertedType = Optional.empty();
        OptionalInt scale = OptionalInt.empty();
        OptionalInt precision = OptionalInt.empty();
        OptionalInt fieldId = OptionalInt.empty();
        Optional<LogicalType> logicalType = Optional.empty();
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) {
                break;
            }
            lastFieldId = fh.fieldId();
            switch (fh.fieldId()) {
                case 1 -> type = Optional.of(PhysicalType.valueOf(r.readI32()));
                case 2 -> typeLength = OptionalInt.of(r.readI32());
                case 3 -> repetitionType = Optional.of(FieldRepetitionType.valueOf(r.readI32()));
                case 4 -> name = r.readString();
                case 5 -> numChildren = OptionalInt.of(r.readI32());
                case 6 -> convertedType = Optional.of(ConvertedType.valueOf(r.readI32()));
                case 7 -> scale = OptionalInt.of(r.readI32());
                case 8 -> precision = OptionalInt.of(r.readI32());
                case 9 -> fieldId = OptionalInt.of(r.readI32());
                case 10 -> logicalType = Optional.of(LogicalTypeDeserializer.read(r));
                default -> r.skipField(fh.type());
            }
        }
        return new SchemaElement(
                type,
                typeLength,
                repetitionType,
                name,
                numChildren,
                convertedType,
                scale,
                precision,
                logicalType,
                fieldId);
    }
}
