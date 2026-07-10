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

import io.tileverse.parquetry.format.PageLocation;

/**
 * Deserializer for the Thrift {@code PageLocation} struct.
 *
 * <pre>
 * struct PageLocation {
 *   1: required i64 offset
 *   2: required i32 compressed_page_size
 *   3: required i64 first_row_index
 * }
 * </pre>
 */
final class PageLocationDeserializer {

    private PageLocationDeserializer() {}

    static PageLocation read(CompactProtocolReader r) throws IOException {
        long offset = 0;
        int compressedPageSize = 0;
        long firstRowIndex = 0;
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) {
                break;
            }
            lastFieldId = fh.fieldId();
            switch (fh.fieldId()) {
                case 1 -> offset = r.readI64();
                case 2 -> compressedPageSize = r.readI32();
                case 3 -> firstRowIndex = r.readI64();
                default -> r.skipField(fh.type());
            }
        }
        return new PageLocation(offset, compressedPageSize, firstRowIndex);
    }
}
