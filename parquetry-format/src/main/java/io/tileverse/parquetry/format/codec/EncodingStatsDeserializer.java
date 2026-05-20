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

import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.EncodingStats;
import io.tileverse.parquetry.format.PageType;

/**
 * Deserializer for the Thrift {@code PageEncodingStats} struct (our model calls it {@link EncodingStats}).
 *
 * <pre>
 * struct PageEncodingStats {
 *   1: required PageType page_type
 *   2: required Encoding encoding
 *   3: required i32 count
 * }
 * </pre>
 */
final class EncodingStatsDeserializer {

    private EncodingStatsDeserializer() {}

    static EncodingStats read(CompactProtocolReader r) throws IOException {
        PageType pageType = PageType.DATA_PAGE;
        Encoding encoding = Encoding.PLAIN;
        int count = 0;
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) {
                break;
            }
            lastFieldId = fh.fieldId();
            switch (fh.fieldId()) {
                case 1 -> pageType = PageType.valueOf(r.readI32());
                case 2 -> encoding = Encoding.valueOf(r.readI32());
                case 3 -> count = r.readI32();
                default -> r.skipField(fh.type());
            }
        }
        return new EncodingStats(pageType, encoding, count);
    }
}
