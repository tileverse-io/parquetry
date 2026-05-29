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
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.format.ColumnOrder;
import io.tileverse.parquetry.format.FileMetaData;

/**
 * Serializer mirror of {@link FileMetaDataDeserializer}.
 *
 * <p>Field 5 ({@code key_value_metadata}) is modelled as a plain list (not Optional) by both the record and the
 * deserializer, so we emit it only when non-empty so the round-trip wire bytes match the read path.
 *
 * <p>Fields 8 ({@code encryption_algorithm}) and 9 ({@code footer_signing_key_metadata}) belong to deferred encryption
 * support and are intentionally not emitted - the deserializer skips them on read.
 */
final class FileMetaDataSerializer {

    private FileMetaDataSerializer() {}

    static void serialize(CompactProtocolWriter w, FileMetaData fmd) throws IOException {
        w.writeStructBegin();
        w.writeI32Field((short) 1, fmd.version());
        w.writeListField((short) 2, CompactType.STRUCT, fmd.schema(), SchemaElementSerializer::serialize);
        w.writeI64Field((short) 3, fmd.numRows());
        w.writeListField((short) 4, CompactType.STRUCT, fmd.rowGroups(), RowGroupSerializer::serialize);
        if (!fmd.keyValueMetadata().isEmpty()) {
            w.writeListField((short) 5, CompactType.STRUCT, fmd.keyValueMetadata(), KeyValueSerializer::serialize);
        }
        Optional<String> createdBy = fmd.createdBy();
        if (createdBy.isPresent()) {
            w.writeStringField((short) 6, createdBy.get());
        }
        Optional<List<ColumnOrder>> columnOrders = fmd.columnOrders();
        if (columnOrders.isPresent()) {
            w.writeListField((short) 7, CompactType.STRUCT, columnOrders.get(), ColumnOrderSerializer::serialize);
        }
        // Fields 8 and 9 (encryption) intentionally not emitted.
        w.writeFieldStop();
        w.writeStructEnd();
    }
}
