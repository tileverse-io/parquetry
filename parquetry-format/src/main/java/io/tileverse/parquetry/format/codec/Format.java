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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import io.tileverse.storage.RangeReader;

import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageHeader;

/**
 * Public API facade for reading Parquet file metadata and page-level structures.
 *
 * <p>The primary entry point is {@link #readFooter(RangeReader)}, which locates and decodes the {@code FileMetaData}
 * Thrift struct from the trailing footer of a Parquet file.
 *
 * <p>Additional methods support reading page-level structures:
 *
 * <ul>
 *   <li>{@link #readPageHeader(InputStream)} - decodes a page header from a column chunk stream
 *   <li>{@link #readColumnIndex(RangeReader, long, int)} - decodes the column-level min/max index
 *   <li>{@link #readOffsetIndex(RangeReader, long, int)} - decodes the per-page byte offset index
 * </ul>
 *
 * <p>Parquet file layout:
 *
 * <pre>
 *   [4-byte magic "PAR1"]
 *   [row group data ...]
 *   [FileMetaData Thrift compact bytes]
 *   [4-byte footer length (little-endian)]
 *   [4-byte magic "PAR1"]
 * </pre>
 */
public final class Format {

    private static final byte[] MAGIC = {'P', 'A', 'R', '1'};
    private static final byte[] MAGIC_ENCRYPTED = {'P', 'A', 'R', 'E'};

    /** Minimum valid file: 4-byte header magic + 4-byte footer length + 4-byte tail magic. */
    private static final int MIN_FILE_SIZE = 12;

    private Format() {}

    /**
     * Reads a {@link PageHeader} from the given {@link InputStream}.
     *
     * <p>Each page in a column chunk is preceded by a Thrift-encoded {@code PageHeader} struct. The caller positions
     * the stream at the start of the header (typically the first byte after the column chunk's {@code data_page_offset}
     * or just after the previous page's payload).
     *
     * @param in stream positioned at the start of a page header; caller retains ownership
     * @return the decoded PageHeader
     * @throws IOException if Thrift decoding fails or the stream ends prematurely
     */
    public static PageHeader readPageHeader(InputStream in) throws IOException {
        return PageHeaderDeserializer.read(new CompactProtocolReader(in));
    }

    /**
     * Reads the {@link ColumnIndex} for a column chunk from a Parquet file.
     *
     * <p>The {@code offset} and {@code length} are taken from {@code ColumnChunk.columnIndexOffset} and
     * {@code ColumnChunk.columnIndexLength}.
     *
     * @param reader source of byte-range reads for the file; caller retains ownership
     * @param offset byte offset of the ColumnIndex in the file
     * @param length byte length of the ColumnIndex
     * @return the decoded ColumnIndex
     * @throws IOException if reading or Thrift decoding fails
     */
    public static ColumnIndex readColumnIndex(RangeReader reader, long offset, int length) throws IOException {
        ByteBuffer buf = reader.readRange(offset, length);
        buf.flip();
        ByteArrayInputStream bin =
                new ByteArrayInputStream(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
        return ColumnIndexDeserializer.read(new CompactProtocolReader(bin));
    }

    /**
     * Reads the {@link OffsetIndex} for a column chunk from a Parquet file.
     *
     * <p>The {@code offset} and {@code length} are taken from {@code ColumnChunk.offsetIndexOffset} and
     * {@code ColumnChunk.offsetIndexLength}.
     *
     * @param reader source of byte-range reads for the file; caller retains ownership
     * @param offset byte offset of the OffsetIndex in the file
     * @param length byte length of the OffsetIndex
     * @return the decoded OffsetIndex
     * @throws IOException if reading or Thrift decoding fails
     */
    public static OffsetIndex readOffsetIndex(RangeReader reader, long offset, int length) throws IOException {
        ByteBuffer buf = reader.readRange(offset, length);
        buf.flip();
        ByteArrayInputStream bin =
                new ByteArrayInputStream(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
        return OffsetIndexDeserializer.read(new CompactProtocolReader(bin));
    }

    /**
     * Reads the {@link FileMetaData} footer from a Parquet file.
     *
     * @param reader source of byte-range reads for the file; caller retains ownership
     * @return the decoded FileMetaData
     * @throws IOException if the file is too small, the magic bytes are wrong, the file uses encryption (PARE magic),
     *     or Thrift decoding fails
     */
    public static FileMetaData readFooter(RangeReader reader) throws IOException {
        long size = reader.size().orElseThrow(() -> new IOException("Cannot determine file size"));
        if (size < MIN_FILE_SIZE) {
            throw new IOException("File too small to be a Parquet file: " + size + " bytes");
        }
        ByteBuffer tail = reader.readRange(size - 8, 8);
        tail.flip();
        tail.order(ByteOrder.LITTLE_ENDIAN);
        int footerLen = tail.getInt();
        byte[] magic = new byte[4];
        tail.get(magic);
        if (Arrays.equals(magic, MAGIC_ENCRYPTED)) {
            throw new IOException("Encrypted file (PARE magic); requires parquetry-encryption module");
        }
        if (!Arrays.equals(magic, MAGIC)) {
            throw new IOException("Not a Parquet file (bad magic): " + new String(magic));
        }
        if (footerLen <= 0) {
            throw new IOException("Invalid footer length: " + footerLen);
        }
        long footerStart = size - 8 - footerLen;
        if (footerStart < 0) {
            throw new IOException("Footer length " + footerLen + " extends before start of file");
        }
        ByteBuffer footerBytes = reader.readRange(footerStart, footerLen);
        footerBytes.flip();
        ByteArrayInputStream bin = new ByteArrayInputStream(
                footerBytes.array(), footerBytes.arrayOffset() + footerBytes.position(), footerBytes.remaining());
        CompactProtocolReader cpr = new CompactProtocolReader(bin);
        return FileMetaDataDeserializer.read(cpr);
    }
}
