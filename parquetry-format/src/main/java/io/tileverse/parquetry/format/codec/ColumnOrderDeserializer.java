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
 * Deserializer for the Thrift {@code ColumnOrder} union.
 *
 * <pre>
 * union ColumnOrder {
 *   1: TypeDefinedOrder TYPE_ORDER                  // empty struct
 *   2: IEEE754TotalOrder IEEE_754_TOTAL_ORDER       // empty struct
 *   3: Int96TimestampOrder INT96_TIMESTAMP_ORDER    // empty struct
 * }
 * </pre>
 *
 * <p>A case added by a newer writer reads as {@link ColumnOrder.Unknown} with its field id; its payload is skipped. A
 * union naming no case reads as {@link ColumnOrder.Unknown} with {@link ColumnOrder.Unknown#NO_CASE}, an undefined
 * order as in parquet-java.
 */
final class ColumnOrderDeserializer {

    private static final int TYPE_ORDER = 1;
    private static final int IEEE_754_TOTAL_ORDER = 2;
    private static final int INT96_TIMESTAMP_ORDER = 3;

    private ColumnOrderDeserializer() {}

    static ColumnOrder read(CompactProtocolReader r) throws IOException {
        FieldHeader fh = r.readFieldHeader(0);
        if (fh.isStop()) {
            return new ColumnOrder.Unknown(ColumnOrder.Unknown.NO_CASE);
        }
        r.skipField(fh.type());
        skipRemainingFields(r, fh.fieldId());
        return orderOf(fh.fieldId());
    }

    private static ColumnOrder orderOf(short fieldId) {
        return switch (fieldId) {
            case TYPE_ORDER -> new ColumnOrder.TypeDefined();
            case IEEE_754_TOTAL_ORDER -> new ColumnOrder.Ieee754TotalOrder();
            case INT96_TIMESTAMP_ORDER -> new ColumnOrder.Int96TimestampOrder();
            default -> new ColumnOrder.Unknown(fieldId);
        };
    }

    /** Consumes the further fields held by a malformed union, up to and including the STOP closing the union. */
    private static void skipRemainingFields(CompactProtocolReader r, short lastFieldId) throws IOException {
        short previous = lastFieldId;
        while (true) {
            FieldHeader extra = r.readFieldHeader(previous);
            if (extra.isStop()) {
                return;
            }
            r.skipField(extra.type());
            previous = extra.fieldId();
        }
    }
}
