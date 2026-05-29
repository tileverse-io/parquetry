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
package io.tileverse.parquetry.format.codec;

import java.io.IOException;
import java.util.Optional;

import io.tileverse.parquetry.format.ConvertedType;
import io.tileverse.parquetry.format.FieldRepetitionType;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.SchemaElement;

/** Serializer mirror of {@link SchemaElementDeserializer}. */
final class SchemaElementSerializer {

    private SchemaElementSerializer() {}

    static void serialize(CompactProtocolWriter w, SchemaElement se) throws IOException {
        w.writeStructBegin();
        Optional<PhysicalType> type = se.type();
        if (type.isPresent()) {
            w.writeI32Field((short) 1, type.get().value());
        }
        if (se.typeLength().isPresent()) {
            w.writeI32Field((short) 2, se.typeLength().getAsInt());
        }
        Optional<FieldRepetitionType> repetitionType = se.repetitionType();
        if (repetitionType.isPresent()) {
            w.writeI32Field((short) 3, repetitionType.get().value());
        }
        w.writeStringField((short) 4, se.name());
        if (se.numChildren().isPresent()) {
            w.writeI32Field((short) 5, se.numChildren().getAsInt());
        }
        Optional<ConvertedType> convertedType = se.convertedType();
        if (convertedType.isPresent()) {
            w.writeI32Field((short) 6, convertedType.get().value());
        }
        if (se.scale().isPresent()) {
            w.writeI32Field((short) 7, se.scale().getAsInt());
        }
        if (se.precision().isPresent()) {
            w.writeI32Field((short) 8, se.precision().getAsInt());
        }
        if (se.fieldId().isPresent()) {
            w.writeI32Field((short) 9, se.fieldId().getAsInt());
        }
        Optional<LogicalType> logicalType = se.logicalType();
        if (logicalType.isPresent()) {
            w.writeFieldBegin((short) 10, CompactType.STRUCT);
            LogicalTypeSerializer.serialize(w, logicalType.get());
        }
        w.writeFieldStop();
        w.writeStructEnd();
    }
}
