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
import java.io.OutputStream;
import java.io.UncheckedIOException;

import io.tileverse.parquetry.format.BloomFilterHeader;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageHeader;

/**
 * Low-level encoder facade: writes the four top-level Parquet metadata structures to a raw {@link OutputStream} as
 * Thrift compact bytes. Symmetric with {@link ParquetFormatDeserializer}. Callers that need to embed the bytes into a
 * custom transport (sliced {@code ByteBuffer}, encrypted envelope, ...) use this directly; callers writing to a regular
 * file should use the higher-level helpers in {@link io.tileverse.parquetry.format.ParquetFormat}.
 */
public final class ParquetFormatSerializer {

    private ParquetFormatSerializer() {}

    /**
     * Writes a {@link FileMetaData} record to {@code out} as Thrift compact bytes.
     *
     * @param out stream to receive the encoded bytes; caller retains ownership
     * @param footer the file-level metadata to encode
     * @throws UncheckedIOException if the underlying {@link OutputStream} fails
     */
    public static void writeFileMetaData(OutputStream out, FileMetaData footer) {
        try {
            FileMetaDataSerializer.serialize(new CompactProtocolWriter(out), footer);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write FileMetaData", e);
        }
    }

    /**
     * Writes a {@link PageHeader} to {@code out} as Thrift compact bytes.
     *
     * @param out stream to receive the encoded bytes; caller retains ownership
     * @param header the page header to encode
     * @throws UncheckedIOException if the underlying {@link OutputStream} fails
     */
    public static void writePageHeader(OutputStream out, PageHeader header) {
        try {
            PageHeaderSerializer.serialize(new CompactProtocolWriter(out), header);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write PageHeader", e);
        }
    }

    /**
     * Writes a {@link ColumnIndex} to {@code out} as Thrift compact bytes.
     *
     * @param out stream to receive the encoded bytes; caller retains ownership
     * @param index the column index to encode
     * @throws UncheckedIOException if the underlying {@link OutputStream} fails
     */
    public static void writeColumnIndex(OutputStream out, ColumnIndex index) {
        try {
            ColumnIndexSerializer.serialize(new CompactProtocolWriter(out), index);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write ColumnIndex", e);
        }
    }

    /**
     * Writes an {@link OffsetIndex} to {@code out} as Thrift compact bytes.
     *
     * @param out stream to receive the encoded bytes; caller retains ownership
     * @param index the offset index to encode
     * @throws UncheckedIOException if the underlying {@link OutputStream} fails
     */
    public static void writeOffsetIndex(OutputStream out, OffsetIndex index) {
        try {
            OffsetIndexSerializer.serialize(new CompactProtocolWriter(out), index);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write OffsetIndex", e);
        }
    }

    /**
     * Writes a {@link BloomFilterHeader} to {@code out} as Thrift compact bytes. The bitset bytes follow this header in
     * the file layout but are emitted by the caller; this method only encodes the header itself.
     *
     * @param out stream to receive the encoded bytes; caller retains ownership
     * @param header the bloom filter header to encode
     * @throws UncheckedIOException if the underlying {@link OutputStream} fails
     */
    public static void writeBloomFilterHeader(OutputStream out, BloomFilterHeader header) {
        try {
            BloomFilterHeaderSerializer.serialize(new CompactProtocolWriter(out), header);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write BloomFilterHeader", e);
        }
    }
}
