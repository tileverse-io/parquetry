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

import io.tileverse.parquetry.format.DictionaryPageHeader;
import io.tileverse.parquetry.format.Encoding;

/**
 * Deserializer for the Thrift {@code DictionaryPageHeader} struct.
 *
 * <pre>
 * struct DictionaryPageHeader {
 *   1: required i32 num_values
 *   2: required Encoding encoding
 *   3: optional bool is_sorted
 * }
 * </pre>
 */
final class DictionaryPageHeaderDeserializer {

    private DictionaryPageHeaderDeserializer() {}

    static DictionaryPageHeader read(CompactProtocolReader r) throws IOException {
        int numValues = 0;
        Encoding encoding = Encoding.PLAIN;
        boolean isSorted = false;
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) {
                break;
            }
            lastFieldId = fh.fieldId();
            switch (fh.fieldId()) {
                case 1 -> numValues = r.readI32();
                case 2 -> encoding = Encoding.valueOf(r.readI32());
                case 3 -> isSorted = fh.type() == CompactType.BOOLEAN_TRUE;
                default -> r.skipField(fh.type());
            }
        }
        return new DictionaryPageHeader(numValues, encoding, isSorted);
    }
}
