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
import java.io.InputStream;
import java.io.UncheckedIOException;

import io.tileverse.parquetry.format.BloomFilterHeader;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.ParquetFormatException;

/**
 * Low-level decoder facade: reads the four Parquet metadata structures from a raw {@link InputStream} containing the
 * Thrift-compact bytes of the corresponding record. Callers that already have the bytes in hand (slicing them out of a
 * {@code ByteBuffer}, a memory-mapped file, or a custom transport) use this directly; callers reading from a
 * {@code RangeReader} use {@link io.tileverse.parquetry.format.ParquetFormat} instead, which knows the file layout and
 * delegates here for the actual decode.
 */
public final class ParquetFormatDeserializer {

    private ParquetFormatDeserializer() {}

    /**
     * Reads a {@link FileMetaData} record from {@code in}.
     *
     * @param in stream positioned at the start of the Thrift-compact-encoded {@code FileMetaData}
     * @return the decoded {@link FileMetaData}
     * @throws ParquetFormatException if the bytes don't conform to the {@code FileMetaData} Thrift layout
     * @throws UncheckedIOException if the underlying {@link InputStream} fails
     */
    public static FileMetaData readFileMetaData(InputStream in) {
        try {
            return FileMetaDataDeserializer.read(new CompactProtocolReader(in));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read FileMetaData", e);
        }
    }

    /**
     * Reads a {@link PageHeader} from {@code in}. Each data / dictionary / index page is preceded by one of these.
     *
     * @param in stream positioned at the start of a page header
     * @return the decoded {@link PageHeader}
     * @throws ParquetFormatException if the bytes don't conform to the {@code PageHeader} Thrift layout
     * @throws UncheckedIOException if the underlying {@link InputStream} fails
     */
    public static PageHeader readPageHeader(InputStream in) {
        try {
            return PageHeaderDeserializer.read(new CompactProtocolReader(in));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read PageHeader", e);
        }
    }

    /**
     * Reads a {@link ColumnIndex} from {@code in}. ColumnIndex bytes live at {@code ColumnChunk.column_index_offset} in
     * the file.
     *
     * @param in stream positioned at the start of a Thrift-compact-encoded {@code ColumnIndex}
     * @return the decoded {@link ColumnIndex}
     * @throws ParquetFormatException if the bytes don't conform to the {@code ColumnIndex} Thrift layout
     * @throws UncheckedIOException if the underlying {@link InputStream} fails
     */
    public static ColumnIndex readColumnIndex(InputStream in) {
        try {
            return ColumnIndexDeserializer.read(new CompactProtocolReader(in));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read ColumnIndex", e);
        }
    }

    /**
     * Reads an {@link OffsetIndex} from {@code in}. OffsetIndex bytes live at {@code ColumnChunk.offset_index_offset}
     * in the file.
     *
     * @param in stream positioned at the start of a Thrift-compact-encoded {@code OffsetIndex}
     * @return the decoded {@link OffsetIndex}
     * @throws ParquetFormatException if the bytes don't conform to the {@code OffsetIndex} Thrift layout
     * @throws UncheckedIOException if the underlying {@link InputStream} fails
     */
    public static OffsetIndex readOffsetIndex(InputStream in) {
        try {
            return OffsetIndexDeserializer.read(new CompactProtocolReader(in));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read OffsetIndex", e);
        }
    }

    /**
     * Reads a {@link BloomFilterHeader} from {@code in}. The header lives at
     * {@code ColumnMetaData.bloom_filter_offset}; the bitset bytes follow immediately and span
     * {@code BloomFilterHeader.numBytes} bytes.
     *
     * @param in stream positioned at the start of a Thrift-compact-encoded {@code BloomFilterHeader}
     * @return the decoded {@link BloomFilterHeader}
     * @throws ParquetFormatException if the bytes don't conform to the {@code BloomFilterHeader} Thrift layout or
     *     declare an unknown algorithm / hash / compression variant
     * @throws UncheckedIOException if the underlying {@link InputStream} fails
     */
    public static BloomFilterHeader readBloomFilterHeader(InputStream in) {
        try {
            return BloomFilterHeaderDeserializer.read(new CompactProtocolReader(in));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read BloomFilterHeader", e);
        }
    }
}
