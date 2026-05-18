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
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.crypto.ColumnCryptoMetaData;

/**
 * Deserializer for the Thrift {@code ColumnChunk} struct.
 *
 * <pre>
 * struct ColumnChunk {
 *   1: optional string file_path
 *   2: required i64 file_offset
 *   3: optional ColumnMetaData meta_data
 *   4: optional i64 offset_index_offset
 *   5: optional i32 offset_index_length
 *   6: optional i64 column_index_offset
 *   7: optional i32 column_index_length
 *   8: optional ColumnCryptoMetaData crypto_metadata
 *   9: optional binary encrypted_column_metadata
 * }
 * </pre>
 */
final class ColumnChunkDeserializer {

    private ColumnChunkDeserializer() {}

    static ColumnChunk read(CompactProtocolReader r) throws IOException {
        Optional<String> filePath = Optional.empty();
        long fileOffset = 0;
        Optional<ColumnMetaData> metaData = Optional.empty();
        OptionalLong offsetIndexOffset = OptionalLong.empty();
        OptionalInt offsetIndexLength = OptionalInt.empty();
        OptionalLong columnIndexOffset = OptionalLong.empty();
        OptionalInt columnIndexLength = OptionalInt.empty();
        Optional<ColumnCryptoMetaData> cryptoMetadata = Optional.empty();
        Optional<ByteBuffer> encryptedColumnMetadata = Optional.empty();
        int lastFieldId = 0;
        while (true) {
            FieldHeader fh = r.readFieldHeader(lastFieldId);
            if (fh.isStop()) {
                break;
            }
            lastFieldId = fh.fieldId();
            switch (fh.fieldId()) {
                case 1 -> filePath = Optional.of(r.readString());
                case 2 -> fileOffset = r.readI64();
                case 3 -> metaData = Optional.of(ColumnMetaDataDeserializer.read(r));
                case 4 -> offsetIndexOffset = OptionalLong.of(r.readI64());
                case 5 -> offsetIndexLength = OptionalInt.of(r.readI32());
                case 6 -> columnIndexOffset = OptionalLong.of(r.readI64());
                case 7 -> columnIndexLength = OptionalInt.of(r.readI32());
                // Fields 8 and 9 are crypto metadata - skipped
                case 8 -> r.skipField(fh.type());
                case 9 -> encryptedColumnMetadata = Optional.of(r.readBinary());
                default -> r.skipField(fh.type());
            }
        }
        return new ColumnChunk(
                filePath,
                fileOffset,
                metaData,
                offsetIndexOffset,
                offsetIndexLength,
                columnIndexOffset,
                columnIndexLength,
                cryptoMetadata,
                encryptedColumnMetadata);
    }
}
