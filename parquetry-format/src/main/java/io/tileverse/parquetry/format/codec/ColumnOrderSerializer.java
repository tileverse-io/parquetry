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
 * Serializer mirror of {@link ColumnOrderDeserializer}. The only known case is {@link ColumnOrder.TypeDefined} (Thrift
 * field id 1), encoded as an empty nested struct payload.
 */
final class ColumnOrderSerializer {

    private ColumnOrderSerializer() {}

    static void serialize(CompactProtocolWriter w, ColumnOrder order) throws IOException {
        // Union outer struct: one field set with the case id, value is an empty nested struct.
        w.writeStructBegin();
        switch (order) {
            case ColumnOrder.TypeDefined _ -> writeTypeDefined(w);
        }
        w.writeFieldStop();
        w.writeStructEnd();
    }

    private static void writeTypeDefined(CompactProtocolWriter w) throws IOException {
        w.writeFieldBegin((short) 1, CompactType.STRUCT);
        // Nested TypeDefinedOrder struct is empty -> just a STOP byte.
        w.writeStructBegin();
        w.writeFieldStop();
        w.writeStructEnd();
    }
}
