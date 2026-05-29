/*
 * Copyright (c) 2026 Multivers.io
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

import io.tileverse.parquetry.format.DataPageHeader;
import io.tileverse.parquetry.format.DataPageHeaderV2;
import io.tileverse.parquetry.format.DictionaryPageHeader;
import io.tileverse.parquetry.format.PageHeader;

/** Serializer mirror of {@link PageHeaderDeserializer}. */
final class PageHeaderSerializer {

    private PageHeaderSerializer() {}

    static void serialize(CompactProtocolWriter w, PageHeader h) throws IOException {
        w.writeStructBegin();
        w.writeI32Field((short) 1, h.type().value());
        w.writeI32Field((short) 2, h.uncompressedPageSize());
        w.writeI32Field((short) 3, h.compressedPageSize());
        if (h.crc().isPresent()) {
            w.writeI32Field((short) 4, h.crc().getAsInt());
        }
        Optional<DataPageHeader> dataPageHeader = h.dataPageHeader();
        if (dataPageHeader.isPresent()) {
            w.writeFieldBegin((short) 5, CompactType.STRUCT);
            DataPageHeaderSerializer.serialize(w, dataPageHeader.get());
        }
        if (h.indexPageHeader().isPresent()) {
            w.writeFieldBegin((short) 6, CompactType.STRUCT);
            IndexPageHeaderSerializer.serialize(w);
        }
        Optional<DictionaryPageHeader> dictionaryPageHeader = h.dictionaryPageHeader();
        if (dictionaryPageHeader.isPresent()) {
            w.writeFieldBegin((short) 7, CompactType.STRUCT);
            DictionaryPageHeaderSerializer.serialize(w, dictionaryPageHeader.get());
        }
        Optional<DataPageHeaderV2> dataPageHeaderV2 = h.dataPageHeaderV2();
        if (dataPageHeaderV2.isPresent()) {
            w.writeFieldBegin((short) 8, CompactType.STRUCT);
            DataPageHeaderV2Serializer.serialize(w, dataPageHeaderV2.get());
        }
        w.writeFieldStop();
        w.writeStructEnd();
    }
}
