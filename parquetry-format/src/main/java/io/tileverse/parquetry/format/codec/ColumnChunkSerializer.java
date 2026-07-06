/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Optional;

import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;

/**
 * Serializer mirror of {@link ColumnChunkDeserializer}.
 *
 * <p>Field 8 ({@code crypto_metadata}, {@code ColumnCryptoMetaData}) is skipped on read; we honour the same gap on
 * write to keep round-trip equality with the read path.
 */
final class ColumnChunkSerializer {

    private ColumnChunkSerializer() {}

    static void serialize(CompactProtocolWriter w, ColumnChunk cc) throws IOException {
        w.writeStructBegin();
        Optional<String> filePath = cc.filePath();
        if (filePath.isPresent()) {
            w.writeStringField((short) 1, filePath.get());
        }
        w.writeI64Field((short) 2, cc.fileOffset());
        Optional<ColumnMetaData> metaData = cc.metaData();
        if (metaData.isPresent()) {
            w.writeFieldBegin((short) 3, CompactType.STRUCT);
            ColumnMetaDataSerializer.serialize(w, metaData.get());
        }
        if (cc.offsetIndexOffset().isPresent()) {
            w.writeI64Field((short) 4, cc.offsetIndexOffset().getAsLong());
        }
        if (cc.offsetIndexLength().isPresent()) {
            w.writeI32Field((short) 5, cc.offsetIndexLength().getAsInt());
        }
        if (cc.columnIndexOffset().isPresent()) {
            w.writeI64Field((short) 6, cc.columnIndexOffset().getAsLong());
        }
        if (cc.columnIndexLength().isPresent()) {
            w.writeI32Field((short) 7, cc.columnIndexLength().getAsInt());
        }
        // Field 8 (crypto_metadata) intentionally elided: skipped on read; round-trip support is future work.
        if (cc.encryptedColumnMetadata() != MemorySegment.NULL) {
            w.writeBinaryField((short) 9, cc.encryptedColumnMetadata().toArray(JAVA_BYTE));
        }
        w.writeFieldStop();
        w.writeStructEnd();
    }
}
