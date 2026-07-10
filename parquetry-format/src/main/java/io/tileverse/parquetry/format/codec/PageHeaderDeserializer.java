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
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.format.DataPageHeader;
import io.tileverse.parquetry.format.DataPageHeaderV2;
import io.tileverse.parquetry.format.DictionaryPageHeader;
import io.tileverse.parquetry.format.IndexPageHeader;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageType;

/**
 * Deserializer for the Thrift {@code PageHeader} struct.
 *
 * <pre>
 * struct PageHeader {
 *   1: required PageType type
 *   2: required i32 uncompressed_page_size
 *   3: required i32 compressed_page_size
 *   4: optional i32 crc
 *   5: optional DataPageHeader data_page_header
 *   6: optional IndexPageHeader index_page_header
 *   7: optional DictionaryPageHeader dictionary_page_header
 *   8: optional DataPageHeaderV2 data_page_header_v2
 * }
 * </pre>
 */
final class PageHeaderDeserializer {

    private PageHeaderDeserializer() {}

    static PageHeader read(CompactProtocolReader r) throws IOException {
        PageType type = PageType.DATA_PAGE;
        int uncompressedPageSize = 0;
        int compressedPageSize = 0;
        OptionalInt crc = OptionalInt.empty();
        Optional<DataPageHeader> dataPageHeader = Optional.empty();
        Optional<IndexPageHeader> indexPageHeader = Optional.empty();
        Optional<DictionaryPageHeader> dictionaryPageHeader = Optional.empty();
        Optional<DataPageHeaderV2> dataPageHeaderV2 = Optional.empty();
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) {
                break;
            }
            lastFieldId = fh.fieldId();
            switch (fh.fieldId()) {
                case 1 -> type = PageType.valueOf(r.readI32());
                case 2 -> uncompressedPageSize = r.readI32();
                case 3 -> compressedPageSize = r.readI32();
                case 4 -> crc = OptionalInt.of(r.readI32());
                case 5 -> dataPageHeader = Optional.of(DataPageHeaderDeserializer.read(r));
                case 6 -> indexPageHeader = Optional.of(IndexPageHeaderDeserializer.read(r));
                case 7 -> dictionaryPageHeader = Optional.of(DictionaryPageHeaderDeserializer.read(r));
                case 8 -> dataPageHeaderV2 = Optional.of(DataPageHeaderV2Deserializer.read(r));
                default -> r.skipField(fh.type());
            }
        }
        return new PageHeader(
                type,
                uncompressedPageSize,
                compressedPageSize,
                crc,
                dataPageHeader,
                indexPageHeader,
                dictionaryPageHeader,
                dataPageHeaderV2);
    }
}
