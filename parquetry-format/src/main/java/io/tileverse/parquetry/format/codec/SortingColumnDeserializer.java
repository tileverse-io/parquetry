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

import io.tileverse.parquetry.format.RowGroup;

/**
 * Deserializer for the Thrift {@code SortingColumn} struct.
 *
 * <pre>
 * struct SortingColumn {
 *   1: required i32 column_idx
 *   2: required bool descending
 *   3: required bool nulls_first
 * }
 * </pre>
 */
final class SortingColumnDeserializer {

    private SortingColumnDeserializer() {}

    static RowGroup.SortingColumn read(CompactProtocolReader r) throws IOException {
        int columnIdx = 0;
        boolean descending = false;
        boolean nullsFirst = false;
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) {
                break;
            }
            lastFieldId = fh.fieldId();
            switch (fh.fieldId()) {
                case 1 -> columnIdx = r.readI32();
                case 2 -> descending = fh.type() == CompactType.BOOLEAN_TRUE;
                case 3 -> nullsFirst = fh.type() == CompactType.BOOLEAN_TRUE;
                default -> r.skipField(fh.type());
            }
        }
        return new RowGroup.SortingColumn(columnIdx, descending, nullsFirst);
    }
}
