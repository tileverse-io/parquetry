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
package io.tileverse.parquetry.format;

import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import io.tileverse.storage.RangeReader;

import io.tileverse.parquetry.format.codec.ParquetFormatDeserializer;

/**
 * Reads Parquet file metadata and page-level structures from a {@link RangeReader}.
 *
 * <p>{@link #readFooter(RangeReader)} locates the {@code FileMetaData} Thrift struct at the end of a Parquet file and
 * decodes it. The page-level methods read the corresponding structures at offsets the caller has obtained from the
 * footer (typically {@code ColumnChunk.column_index_offset}, {@code .offset_index_offset}, or
 * {@code .data_page_offset}).
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
 *
 * <p>If you already have the raw Thrift bytes in hand (e.g. by slicing a {@code ByteBuffer} from a custom transport),
 * call {@link ParquetFormatDeserializer} directly. The methods here pull bytes off a {@code RangeReader} and then
 * delegate to it.
 */
public final class ParquetFormat {

    private static final byte[] MAGIC = {'P', 'A', 'R', '1'};
    private static final byte[] MAGIC_ENCRYPTED = {'P', 'A', 'R', 'E'};

    /** Minimum valid file: 4-byte header magic + 4-byte footer length + 4-byte tail magic. */
    private static final int MIN_FILE_SIZE = 12;

    private ParquetFormat() {}

    /**
     * Reads a {@link PageHeader} from {@code in}.
     *
     * <p>Each page in a column chunk is preceded by a Thrift-encoded {@code PageHeader} struct. The caller positions
     * the stream at the start of the header (typically the first byte after the column chunk's {@code data_page_offset}
     * or just after the previous page's payload).
     *
     * @param in stream positioned at the start of a page header; caller retains ownership
     * @return the decoded {@link PageHeader}
     * @throws ParquetFormatException if the bytes don't conform to the Thrift-compact / Parquet page-header layout
     * @throws UncheckedIOException if the underlying {@code InputStream} fails or ends prematurely
     */
    public static PageHeader readPageHeader(InputStream in) {
        return ParquetFormatDeserializer.readPageHeader(in);
    }

    /**
     * Reads the {@link ColumnIndex} for a column chunk from a Parquet file. The {@code offset} and {@code length} are
     * taken from {@code ColumnChunk.column_index_offset} and {@code .column_index_length}.
     *
     * @param reader source of byte-range reads for the file; caller retains ownership
     * @param offset byte offset of the ColumnIndex in the file
     * @param length byte length of the ColumnIndex
     * @return the decoded {@link ColumnIndex}
     * @throws ParquetFormatException if the bytes at {@code offset} don't conform to the {@code ColumnIndex} layout
     * @throws UncheckedIOException if the underlying {@link RangeReader} read fails
     */
    public static ColumnIndex readColumnIndex(RangeReader reader, long offset, int length) {
        ByteBuffer buf = reader.readRange(offset, length);
        buf.flip();
        return ParquetFormatDeserializer.readColumnIndex(toInputStream(buf));
    }

    /**
     * Reads the {@link OffsetIndex} for a column chunk from a Parquet file. The {@code offset} and {@code length} are
     * taken from {@code ColumnChunk.offset_index_offset} and {@code .offset_index_length}.
     *
     * @param reader source of byte-range reads for the file; caller retains ownership
     * @param offset byte offset of the OffsetIndex in the file
     * @param length byte length of the OffsetIndex
     * @return the decoded {@link OffsetIndex}
     * @throws ParquetFormatException if the bytes at {@code offset} don't conform to the {@code OffsetIndex} layout
     * @throws UncheckedIOException if the underlying {@link RangeReader} read fails
     */
    public static OffsetIndex readOffsetIndex(RangeReader reader, long offset, int length) {
        ByteBuffer buf = reader.readRange(offset, length);
        buf.flip();
        return ParquetFormatDeserializer.readOffsetIndex(toInputStream(buf));
    }

    /**
     * Reads the {@link FileMetaData} footer from a Parquet file.
     *
     * @param reader source of byte-range reads for the file; caller retains ownership
     * @return the decoded {@link FileMetaData}
     * @throws ParquetFormatException if the file is too small, has wrong magic bytes, uses encryption (PARE magic),
     *     declares an invalid footer length, or contains Thrift bytes that don't match the {@code FileMetaData} layout
     * @throws UncheckedIOException if the underlying {@link RangeReader} read fails, or the reader cannot determine the
     *     file size
     */
    public static FileMetaData readFooter(RangeReader reader) {
        final long size =
                reader.size().orElseThrow(() -> new ParquetFormatException("RangeReader cannot determine file size"));
        if (size < MIN_FILE_SIZE) {
            throw new ParquetFormatException("File too small to be a Parquet file: " + size + " bytes");
        }
        ByteBuffer tail = reader.readRange(size - 8, 8);
        tail.flip();
        tail.order(ByteOrder.LITTLE_ENDIAN);
        int footerLen = tail.getInt();
        byte[] magic = new byte[4];
        tail.get(magic);
        if (Arrays.equals(magic, MAGIC_ENCRYPTED)) {
            throw new ParquetFormatException("Encrypted file (PARE magic); requires parquetry-encryption module");
        }
        if (!Arrays.equals(magic, MAGIC)) {
            throw new ParquetFormatException("Not a Parquet file (bad magic): " + new String(magic));
        }
        if (footerLen <= 0) {
            throw new ParquetFormatException("Invalid footer length: " + footerLen);
        }
        long footerStart = size - 8 - footerLen;
        if (footerStart < 0) {
            throw new ParquetFormatException("Footer length " + footerLen + " extends before start of file");
        }
        ByteBuffer footerBytes = reader.readRange(footerStart, footerLen);
        footerBytes.flip();
        return ParquetFormatDeserializer.readFileMetaData(toInputStream(footerBytes));
    }

    private static InputStream toInputStream(ByteBuffer buf) {
        return new ByteBufferInputStream(buf);
    }

    private static final class ByteBufferInputStream extends InputStream {

        private final ByteBuffer buffer;

        ByteBufferInputStream(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public int read() {
            if (buffer.hasRemaining()) {
                return buffer.get() & 0xff;
            }
            return -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (buffer.hasRemaining()) {
                int toRead = Math.min(len, buffer.remaining());
                buffer.get(b, off, toRead);
                return toRead;
            }
            return -1;
        }

        @Override
        public int available() {
            return buffer.remaining();
        }
    }
}
