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

import io.tileverse.parquetry.format.IndexPageHeader;

/**
 * Deserializer for the Thrift {@code IndexPageHeader} struct.
 *
 * <pre>
 * struct IndexPageHeader {
 *   // currently empty in parquet.thrift; reserved for future fields
 * }
 * </pre>
 */
final class IndexPageHeaderDeserializer {

    private IndexPageHeaderDeserializer() {}

    static IndexPageHeader read(CompactProtocolReader r) throws IOException {
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) {
                break;
            }
            lastFieldId = fh.fieldId();
            r.skipField(fh.type());
        }
        return new IndexPageHeader();
    }
}
