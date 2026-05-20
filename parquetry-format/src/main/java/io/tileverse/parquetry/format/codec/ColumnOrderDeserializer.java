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
package io.tileverse.parquetry.format.codec;

import java.io.IOException;

import io.tileverse.parquetry.format.ColumnOrder;

/**
 * Deserializer for the Thrift {@code ColumnOrder} union.
 *
 * <p>The union has only one defined variant today:
 *
 * <pre>
 * union ColumnOrder {
 *   1: TypeDefinedOrder TYPE_ORDER  // empty struct
 * }
 * </pre>
 *
 * <p>Any unknown field IDs are skipped for forward compatibility; the result defaults to
 * {@link ColumnOrder.TypeDefined} regardless of the actual variant seen, since there is currently only one valid value
 * in the spec.
 */
final class ColumnOrderDeserializer {

    private ColumnOrderDeserializer() {}

    static ColumnOrder read(CompactProtocolReader r) throws IOException {
        // A union has one field set. Read it and skip its content.
        FieldHeader fh = r.readFieldHeader(0);
        if (fh.isStop()) {
            return new ColumnOrder.TypeDefined();
        }
        // Skip the payload (TypeDefinedOrder is an empty struct; future variants may differ)
        r.skipField(fh.type());
        // Consume the STOP of the union struct
        while (true) {
            FieldHeader extra = r.readFieldHeader(fh.fieldId());
            if (extra.isStop()) {
                break;
            }
            r.skipField(extra.type());
        }
        return new ColumnOrder.TypeDefined();
    }
}
