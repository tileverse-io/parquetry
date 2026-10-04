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
package io.tileverse.parquetry.format.codec;

import java.io.IOException;

import io.tileverse.parquetry.format.ColumnOrder;

/**
 * Serializer mirror of {@link ColumnOrderDeserializer}. Each case is written as its union field id with an empty nested
 * struct payload; a {@link ColumnOrder.Unknown} case keeps the field id read with it, and one naming no case is written
 * back as an empty union.
 */
final class ColumnOrderSerializer {

    private static final short TYPE_ORDER = 1;
    private static final short IEEE_754_TOTAL_ORDER = 2;
    private static final short INT96_TIMESTAMP_ORDER = 3;

    private ColumnOrderSerializer() {}

    static void serialize(CompactProtocolWriter w, ColumnOrder order) throws IOException {
        w.writeStructBegin();
        short fieldId = fieldIdOf(order);
        if (fieldId != ColumnOrder.Unknown.NO_CASE) {
            writeEmptyCase(w, fieldId);
        }
        w.writeFieldStop();
        w.writeStructEnd();
    }

    private static short fieldIdOf(ColumnOrder order) {
        return switch (order) {
            case ColumnOrder.TypeDefined _ -> TYPE_ORDER;
            case ColumnOrder.Ieee754TotalOrder _ -> IEEE_754_TOTAL_ORDER;
            case ColumnOrder.Int96TimestampOrder _ -> INT96_TIMESTAMP_ORDER;
            case ColumnOrder.Unknown unknown -> unknown.fieldId();
        };
    }

    private static void writeEmptyCase(CompactProtocolWriter w, short fieldId) throws IOException {
        w.writeFieldBegin(fieldId, CompactType.STRUCT);
        w.writeStructBegin();
        w.writeFieldStop();
        w.writeStructEnd();
    }
}
