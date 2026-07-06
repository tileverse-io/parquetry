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
package io.tileverse.parquetry.format;

import java.lang.foreign.MemorySegment;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import io.tileverse.parquetry.format.crypto.ColumnCryptoMetaData;
import io.tileverse.parquetry.io.Segments;

import lombok.Builder;

/**
 * Pointer to one column's bytes within a {@link RowGroup}; mirror of {@code ColumnChunk} in {@code parquet.thrift}.
 *
 * <p>Holds the inline {@link ColumnMetaData} (or its encrypted bytes) together with the file-level offsets and lengths
 * of the optional column index, offset index, and bloom filter. {@link #filePath()} is set only when the chunk lives in
 * a different file than the footer.
 *
 * @param filePath path to an external file holding this chunk's column data; empty when the chunk lives in the same
 *     file as the footer
 * @param fileOffset deprecated {@code file_offset}; writers should set to {@code 0} and readers should ignore it
 * @param metaData inline {@link ColumnMetaData}; empty when the chunk is encrypted and the metadata is carried in
 *     {@link #encryptedColumnMetadata()}
 * @param offsetIndexOffset file offset of this chunk's {@link OffsetIndex}; unset when no offset index was written
 * @param offsetIndexLength byte size of this chunk's {@link OffsetIndex}; unset when no offset index was written
 * @param columnIndexOffset file offset of this chunk's {@link ColumnIndex}; unset when no column index was written
 * @param columnIndexLength byte size of this chunk's {@link ColumnIndex}; unset when no column index was written
 * @param cryptoMetadata crypto metadata describing how this chunk is encrypted; empty when the chunk is not encrypted
 * @param encryptedColumnMetadata serialized, encrypted {@link ColumnMetaData} bytes; {@link MemorySegment#NULL} when
 *     {@link #metaData()} is present in cleartext
 */
// S2789: constructor null-tolerates Optional to support the @Builder pattern.
@SuppressWarnings("java:S2789")
@Builder
public record ColumnChunk(
        Optional<String> filePath,
        long fileOffset,
        Optional<ColumnMetaData> metaData,
        OptionalLong offsetIndexOffset,
        OptionalInt offsetIndexLength,
        OptionalLong columnIndexOffset,
        OptionalInt columnIndexLength,
        Optional<ColumnCryptoMetaData> cryptoMetadata,
        MemorySegment encryptedColumnMetadata) {

    public ColumnChunk {
        filePath = filePath == null ? Optional.empty() : filePath;
        metaData = metaData == null ? Optional.empty() : metaData;
        offsetIndexOffset = offsetIndexOffset == null ? OptionalLong.empty() : offsetIndexOffset;
        offsetIndexLength = offsetIndexLength == null ? OptionalInt.empty() : offsetIndexLength;
        columnIndexOffset = columnIndexOffset == null ? OptionalLong.empty() : columnIndexOffset;
        columnIndexLength = columnIndexLength == null ? OptionalInt.empty() : columnIndexLength;
        cryptoMetadata = cryptoMetadata == null ? Optional.empty() : cryptoMetadata;
        encryptedColumnMetadata = Segments.readOnlyOrAbsent(encryptedColumnMetadata);
    }
}
