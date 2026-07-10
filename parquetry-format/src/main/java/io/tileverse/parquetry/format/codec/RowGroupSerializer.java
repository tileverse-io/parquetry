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
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.format.RowGroup.SortingColumn;

/** Serializer mirror of {@link RowGroupDeserializer}. */
final class RowGroupSerializer {

    private RowGroupSerializer() {}

    static void serialize(CompactProtocolWriter w, RowGroup rg) throws IOException {
        w.writeStructBegin();
        w.writeListField((short) 1, CompactType.STRUCT, rg.columns(), ColumnChunkSerializer::serialize);
        w.writeI64Field((short) 2, rg.totalByteSize());
        w.writeI64Field((short) 3, rg.numRows());
        Optional<List<SortingColumn>> sortingColumns = rg.sortingColumns();
        if (sortingColumns.isPresent()) {
            w.writeListField((short) 4, CompactType.STRUCT, sortingColumns.get(), SortingColumnSerializer::serialize);
        }
        if (rg.fileOffset().isPresent()) {
            w.writeI64Field((short) 5, rg.fileOffset().getAsLong());
        }
        if (rg.totalCompressedSize().isPresent()) {
            w.writeI64Field((short) 6, rg.totalCompressedSize().getAsLong());
        }
        if (rg.ordinal().isPresent()) {
            // parquet.thrift declares ordinal as i16; the deserializer calls r.readI32() on field 7. Match that.
            w.writeI32Field((short) 7, rg.ordinal().getAsInt());
        }
        w.writeFieldStop();
        w.writeStructEnd();
    }
}
